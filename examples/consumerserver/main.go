package main

import (
	"context"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/core/etrace"
	"github.com/gotomicro/ego/core/etrace/ejaeger"
	"github.com/gotomicro/ego/server/egovernor"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/ego-component/ekafka"
	"github.com/ego-component/ekafka/consumerserver"
)

func main() {
	etrace.SetGlobalTracer(ejaeger.DefaultConfig().Build())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	conf := `
	[kafka]
	debug=true
	brokers = ["192.168.64.65:9091", "192.168.64.65:9092", "192.168.64.65:9093"]
	[kafka.client]
        timeout="3s"
	[kafka.producers.p1]        # 定义了名字为p1的producer
		topic="sre-infra-debug"  # 指定生产消息的topic

	[kafka.consumers.c1]        # 定义了名字为c1的consumer
		topic="sre-infra-debug"  # 指定消费的topic
		groupID="group-1"       # 如果配置了groupID，将初始化为consumerGroup	

	[kafkaConsumerServers.s1]
	debug=true
	consumerName="c1"
`
	// 加载配置文件
	err := econf.LoadFromReader(strings.NewReader(conf), toml.Unmarshal)
	if err != nil {
		panic("LoadFromReader fail," + err.Error())
	}

	app := ego.New().Serve(
		// 可以搭配其他服务模块一起使用
		egovernor.Load("server.governor").Build(),

		// 初始化 Consumer Server
		func() *consumerserver.Component {
			// 依赖 `ekafka` 管理 Kafka consumer
			ec := ekafka.Load("kafka").Build()
			cs := consumerserver.Load("kafkaConsumerServers.s1").Build(
				consumerserver.WithEkafka(ec),
			)

			// 注册处理消息的回调函数
			// cs.OnConsumeEachMessage(func(ctx context.Context, message *ekafka.Message) error {
			// 	fmt.Printf("got a message: %s\n", string(message.Value))
			// 	// 如果返回错误会跳过commit
			// 	return nil
			// })

			cs.SubscribeBatchHandler(func(ctx context.Context, messages []*ekafka.CtxMessage) error {
				// 如果返回错误会跳过commit
				return nil
			}, 3, time.Minute*10)
			return cs
		}(),
		// 还可以启动多个 Consumer Server
	)
	if err := app.Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
