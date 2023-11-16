package ekafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/gotomicro/ego/core/eapp"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/core/emetric"
	"github.com/gotomicro/ego/core/etrace"
	"github.com/gotomicro/ego/core/transport"
	"github.com/gotomicro/ego/core/util/xdebug"
	"github.com/gotomicro/ego/core/util/xstring"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

type ctxStartTimeKey struct{}

type clientProcessFn func(context.Context, Messages, *cmd) error

type cmd struct {
	name string
	res  interface{}
	msg  Message // 响应参数
}

type ClientInterceptor func(oldProcessFn clientProcessFn) (newProcessFn clientProcessFn)

func InterceptorClientChain(interceptors ...ClientInterceptor) ClientInterceptor {
	return func(p clientProcessFn) clientProcessFn {
		chain := p
		for i := len(interceptors) - 1; i >= 0; i-- {
			chain = buildInterceptor(interceptors[i], chain)
		}
		return chain
	}
}

func buildInterceptor(interceptor ClientInterceptor, oldProcess clientProcessFn) clientProcessFn {
	return interceptor(oldProcess)
}

func fixedClientInterceptor(_ string, _ *config) ClientInterceptor {
	return func(next clientProcessFn) clientProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			start := time.Now()
			ctx = context.WithValue(ctx, ctxStartTimeKey{}, start)
			err := next(ctx, msgs, cmd)
			return err
		}
	}
}

func traceClientInterceptor(compName string, c *config) ClientInterceptor {
	tracer := etrace.NewTracer(trace.SpanKindProducer)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
	}
	return func(next clientProcessFn) clientProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			carrier := propagation.MapCarrier{}
			ctx, span := tracer.Start(ctx, fmt.Sprintf("kafka.%s", cmd.name), carrier, trace.WithAttributes(attrs...))
			defer span.End()

			headers := make([]kafka.Header, 0)
			for _, key := range carrier.Keys() {
				headers = append(headers, kafka.Header{
					Key:   key,
					Value: []byte(carrier.Get(key)),
				})
			}
			for _, value := range msgs {
				value.Headers = append(value.Headers, headers...)
				value.Time = time.Now()
			}
			err := next(ctx, msgs, cmd)

			span.SetAttributes(
				semconv.MessagingDestinationKindTopic,
				semconv.MessagingDestinationKey.String(cmd.msg.Topic),
			)

			return err
		}
	}
}

func accessClientInterceptor(compName string, c *config, logger *elog.Component) ClientInterceptor {
	return func(next clientProcessFn) clientProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			loggerKeys := transport.CustomContextKeys()
			fields := make([]elog.Field, 0, 10+len(loggerKeys))

			if c.EnableAccessInterceptor {
				headers := make([]kafka.Header, 0)
				for _, key := range loggerKeys {
					if value := cast.ToString(transport.Value(ctx, key)); value != "" {
						fields = append(fields, elog.FieldCustomKeyValue(key, value))
						headers = append(headers, kafka.Header{
							Key:   key,
							Value: []byte(value),
						})
					}
				}
				for _, value := range msgs {
					value.Headers = append(value.Headers, headers...)
					value.Time = time.Now()
				}
			}

			err := next(ctx, msgs, cmd)
			cost := time.Since(ctx.Value(ctxStartTimeKey{}).(time.Time))
			if c.EnableAccessInterceptor {
				fields = append(fields,
					elog.FieldMethod(cmd.name),
					elog.FieldCost(cost),
				)

				// 开启了链路，那么就记录链路id
				if c.EnableTraceInterceptor && etrace.IsGlobalTracerRegistered() {
					fields = append(fields, elog.FieldTid(etrace.ExtractTraceID(ctx)))
				}
				if c.EnableAccessInterceptorReq {
					fields = append(fields, elog.Any("req", json.RawMessage(xstring.JSON(msgs.ToLog()))))
				}
				if c.EnableAccessInterceptorRes {
					fields = append(fields, elog.Any("res", json.RawMessage(xstring.JSON(cmd.res))))
				}
				if err != nil {
					fields = append(fields, elog.FieldErr(err))
					logger.Error("access", fields...)
				} else {
					logger.Info("access", fields...)
				}
			}

			if !eapp.IsDevelopmentMode() {
				return err
			}
			if err != nil {
				log.Println("[ekafka.response]", xdebug.MakeReqAndResError(fileClientWithLineNum(), compName, fmt.Sprintf("%v", c.Brokers),
					cost, fmt.Sprintf("%s %s %v", cmd.name, cmd.msg.Topic, xstring.JSON(msgs.ToLog())), err.Error()),
				)
			} else {
				log.Println("[ekafka.response]", xdebug.MakeReqAndResInfo(fileClientWithLineNum(), compName, fmt.Sprintf("%v", c.Brokers),
					cost, fmt.Sprintf("%s %s %v", cmd.name, cmd.msg.Topic, xstring.JSON(msgs.ToLog())), fmt.Sprintf("%v", cmd.res)),
				)
			}
			return err
		}
	}
}

func metricClientInterceptor(compName string, config *config) ClientInterceptor {
	return func(next clientProcessFn) clientProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			err := next(ctx, msgs, cmd)
			cost := time.Since(ctx.Value(ctxStartTimeKey{}).(time.Time))
			compNameTopic := fmt.Sprintf("%s.%s", compName, cmd.msg.Topic)
			emetric.ClientHandleHistogram.WithLabelValues("kafka", compNameTopic, cmd.name, strings.Join(config.Brokers, ",")).Observe(cost.Seconds())
			if err != nil {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, cmd.name, strings.Join(config.Brokers, ","), "Error")
				return err
			}
			emetric.ClientHandleCounter.Inc("kafka", compNameTopic, cmd.name, strings.Join(config.Brokers, ","), "OK")
			return nil
		}
	}
}

func fileClientWithLineNum() string {
	// the second caller usually from internal, so set i start from 2
	for i := 2; i < 15; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if (!(strings.Contains(file, "ego-component/ekafka") && strings.HasSuffix(file, "interceptor_client.go")) && !strings.Contains(file, "/ego-component/ekafka/producer.go")) || strings.HasSuffix(file, "_test.go") {
			return file + ":" + strconv.FormatInt(int64(line), 10)
		}
	}
	return ""
}

func compressClientInterceptor(compName string, config *config, logger *elog.Component) ClientInterceptor {
	return func(next clientProcessFn) clientProcessFn {
		return func(ctx context.Context, messages Messages, c *cmd) error {
			var err error
			compressor := GetCompressor(config.CompressType)
			if compressor == nil {
				logger.Error("unknown compressor", zap.String("compName", compName), zap.String("compressType", config.CompressType))
				// 找不到压缩器打印报错日志，不执行压缩
				return next(ctx, messages, c)
			}

			for _, value := range messages {
				// 超过阈值
				if len(value.Value) > config.CompressLimit {
					hasKey := false
					for _, h := range value.Headers {
						if h.Key == compressHeaderKey {
							// 已经存在说明已压缩过 不再重复压缩
							hasKey = true
							break
						}
					}
					if !hasKey {
						value.Headers = append(value.Headers, kafka.Header{
							Key:   compressHeaderKey,
							Value: []byte(config.CompressType),
						})
						value.Value, err = compressor.Compress(value.Value)
						if err != nil {
							// 压缩有异常
							return err
						}
					}
				}
			}
			return next(ctx, messages, c)
		}
	}
}
