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
			// ????????????????????????????????????????????????????????????slice??????
			loggerKeys := transport.CustomContextKeys()

			// kafka ???????????????????????????????????????
			// ??????????????????next???????????????????????????cmd??????msg????????????
			if c.EnableTraceInterceptor {
				carrier := propagation.MapCarrier{}
				// ?????????header?????????????????????producer????????????trace id
				for _, value := range cmd.msg.Headers {
					carrier[value.Key] = string(value.Value)
				}

				// // ?????????context??????????????????trace id?????????????????????????????????
				// // ?????????fetch message????????????context ????????? commit message?????????????????????????????????context??????????????????trace id???
				// propagator := propagation.TraceContext{}
				// propagator.Inject(ctx, carrier)
				var (
					span trace.Span
				)
				ctx, span = tracer.Start(ctx, "kafka", carrier, trace.WithAttributes(attrs...))
				defer span.End()

				span.SetAttributes(
					semconv.MessagingDestinationKindKey.String(cmd.msg.Topic),
				)
			}

			cost := time.Since(ctx.Value(ctxStartTimeKey{}).(time.Time))
			if c.EnableAccessInterceptor {
				var fields = make([]elog.Field, 0, 10+len(loggerKeys))

				fields = append(fields,
					elog.FieldMethod(cmd.name),
					elog.FieldCost(cost),
				)

				// ???????????????????????????????????????id
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
				log.Println("[ekafka.response]", xdebug.MakeReqAndResError(fileServerWithLineNum(), compName,
					fmt.Sprintf("%v", c.Brokers), cost, fmt.Sprintf("%s %v", cmd.name, xstring.JSON(msgs.ToLog())), err.Error()),
				)
			} else {
				log.Println("[ekafka.response]", xdebug.MakeReqAndResInfo(fileServerWithLineNum(), compName,
					fmt.Sprintf("%v", c.Brokers), cost, fmt.Sprintf("%s %v", cmd.name, xstring.JSON(msgs.ToLog())), xstring.JSON(messageToLog(cmd.msg))),
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
