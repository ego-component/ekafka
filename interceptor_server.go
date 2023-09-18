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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

type serverProcessFn func(context.Context, Messages, *cmd) error

type ServerInterceptor func(oldProcessFn serverProcessFn) (newProcessFn serverProcessFn)

func InterceptorServerChain(interceptors ...ServerInterceptor) ServerInterceptor {
	return func(p serverProcessFn) serverProcessFn {
		chain := p
		for i := len(interceptors) - 1; i >= 0; i-- {
			chain = buildServerInterceptor(interceptors[i], chain)
		}
		return chain
	}
}

func buildServerInterceptor(interceptor ServerInterceptor, oldProcess serverProcessFn) serverProcessFn {
	return interceptor(oldProcess)
}

func fixedServerInterceptor(_ string, _ *config) ServerInterceptor {
	return func(next serverProcessFn) serverProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			start := time.Now()
			ctx = context.WithValue(ctx, ctxStartTimeKey{}, start)
			err := next(ctx, msgs, cmd)
			return err
		}
	}
}

func traceServerInterceptor(compName string, c *config) ServerInterceptor {
	return func(next serverProcessFn) serverProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			headers := make([]kafka.Header, 0)

			for _, value := range msgs {
				value.Headers = headers
				value.Time = time.Now()
			}
			err := next(ctx, msgs, cmd)
			return err
		}
	}
}

func accessServerInterceptor(compName string, c *config, logger *elog.Component) ServerInterceptor {
	tracer := etrace.NewTracer(trace.SpanKindConsumer)
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
	}
	return func(next serverProcessFn) serverProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			err := next(ctx, msgs, cmd)
			// 为了性能考虑，如果要加日志字段，需要改变slice大小
			loggerKeys := transport.CustomContextKeys()

			// kafka 比较坑爹，合在一起处理链路
			// 这个地方要放next后面，因为这个时候cmd里的msg才有数据
			if c.EnableTraceInterceptor {
				carrier := propagation.MapCarrier{}
				// 首先看header头里，也就是从producer里传递的trace id
				for _, value := range cmd.msg.Headers {
					carrier[value.Key] = string(value.Value)
				}

				// // 然后从context中获取是否有trace id。如果有的话就直接使用
				// // 通常是fetch message后，会将context 传递给 commit message，那么这个时候，就要从context里拿到对应的trace id。
				// propagator := propagation.TraceContext{}
				// propagator.Inject(ctx, carrier)
				var span trace.Span
				ctx, span = tracer.Start(ctx, fmt.Sprintf("kafka.%s", cmd.name), carrier, trace.WithAttributes(attrs...))
				defer span.End()
				span.SetAttributes(
					semconv.MessagingDestinationKindTopic,
					semconv.MessagingDestinationKey.String(cmd.msg.Topic),
				)
			}

			cost := time.Since(ctx.Value(ctxStartTimeKey{}).(time.Time))
			if c.EnableAccessInterceptor {
				var fields = make([]elog.Field, 0, 10+len(loggerKeys))
				fields = append(fields,
					elog.FieldMethod(cmd.name),
					elog.FieldCost(cost),
				)

				// 开启了链路，那么就记录链路id
				if c.EnableTraceInterceptor && etrace.IsGlobalTracerRegistered() {
					fields = append(fields, elog.FieldTid(etrace.ExtractTraceID(ctx)))

					for _, key := range loggerKeys {
						for _, value := range cmd.msg.Headers {
							if value.Key == key {
								fields = append(fields, elog.FieldCustomKeyValue(key, string(value.Value)))
							}
						}
					}
				}
				if c.EnableAccessInterceptorReq {
					fields = append(fields, elog.Any("req", json.RawMessage(xstring.JSON(msgs.ToLog()))))
				}
				if c.EnableAccessInterceptorRes {
					fields = append(fields, elog.Any("res", json.RawMessage(xstring.JSON(messageToLog(cmd.msg)))))
				}
				logger.Info("access", fields...)
			}

			if !eapp.IsDevelopmentMode() {
				return err
			}
			if err != nil {
				log.Println("[ekafka.response]", xdebug.MakeReqAndResError(fileServerWithLineNum(), compName, fmt.Sprintf("%v", c.Brokers),
					cost, fmt.Sprintf("%s %s %v", cmd.name, cmd.msg.Topic, xstring.JSON(msgs.ToLog())), err.Error()),
				)
			} else {
				log.Println("[ekafka.response]", xdebug.MakeReqAndResInfo(fileServerWithLineNum(), compName, fmt.Sprintf("%v", c.Brokers),
					cost, fmt.Sprintf("%s %s %v", cmd.name, cmd.msg.Topic, xstring.JSON(msgs.ToLog())), xstring.JSON(messageToLog(cmd.msg))),
				)
			}
			return err
		}
	}
}

func metricServerInterceptor(compName string, config *config) ServerInterceptor {
	return func(next serverProcessFn) serverProcessFn {
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

func fileServerWithLineNum() string {
	// the second caller usually from internal, so set i start from 2
	for i := 2; i < 15; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if (!(strings.Contains(file, "ego-component/ekafka") && strings.HasSuffix(file, "interceptor_server.go")) && !strings.Contains(file, "/ego-component/ekafka/consumer.go")) || strings.HasSuffix(file, "_test.go") {
			return file + ":" + strconv.FormatInt(int64(line), 10)
		}
	}
	return ""
}

func compressServerInterceptor(compName string, config *config, logger *elog.Component) ServerInterceptor {
	return func(next serverProcessFn) serverProcessFn {
		return func(ctx context.Context, msgs Messages, cmd *cmd) error {
			err := next(ctx, msgs, cmd)
			if err != nil {
				return err
			}
			var compressor Compressor
			if cmd.res == nil {
				return nil
			}
			value, ok := cmd.res.(*Message)
			if !ok {
				logger.Error("unexpected type", zap.Any("res", cmd.res))
				return nil
			}
			headers := make([]kafka.Header, 0)
			for i := range value.Headers {
				h := value.Headers[i]
				if h.Key == compressHeaderKey {
					compressor = GetCompressor(string(h.Value))
					if compressor == nil {
						logger.Error("unknown compressor", zap.String("compName", compName), zap.String("compressType", config.CompressType))
					}
				} else {
					headers = append(headers, h)
				}
			}
			if compressor != nil {
				// 移除header
				value.Headers = headers
				// 解压
				value.Value, err = compressor.DeCompress(value.Value)
				if err != nil {
					// 解压有异常
					return err
				}
			}

			return nil
		}
	}
}
