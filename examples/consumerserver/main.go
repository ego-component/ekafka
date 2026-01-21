package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego"
	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server/egovernor"
	"github.com/segmentio/kafka-go"

	"github.com/ego-component/ekafka"
)

func main() {
	conf := `
	[kafka]
	debug=true
	brokers=["localhost:9092"]
	[kafka.client]
        timeout="3s"
	[kafka.producers.p1]        # 定义了名字为p1的producer
		topic="topic-1"  # 指定生产消息的topic
	[kafka.producers.p2]        # 定义了名字为p1的producer
		topic="topic-2"  # 指定生产消息的topic

	[kafka.consumers.c1]        # 定义了名字为c1的consumer
		topic="topic-1"  # 指定消费的topic
		groupID="group-1"       # 如果配置了groupID，将初始化为consumerGroup
	[kafka.consumers.c2]        # 定义了名字为c1的consumer
		topic="topic-2"  # 指定消费的topic
		groupID="group-2"       # 如果配置了groupID，将初始化为consumerGroup
	[kafka.consumerServers.cs1]
		consumerName="c1"
	[kafka.consumerServers.cs2]
		consumerName="c2"
`
	// 加载配置文件
	err := econf.LoadFromReader(strings.NewReader(conf), toml.Unmarshal)
	if err != nil {
		panic("LoadFromReader fail," + err.Error())
	}
	kafkaClient := ekafka.Load("kafka").Build()

	// p1 生产消息
	go func() {
		count := 1
		for {
			if err := kafkaClient.Producer("p1").WriteMessages(context.Background(), &ekafka.Message{
				Value: []byte("p1 hello world, count:" + strconv.Itoa(count)),
			}); err != nil {
				elog.Error("produce fail", elog.FieldErr(err))
			}
			count++
			time.Sleep(time.Second)
		}
	}()

	// p2 生产消息
	go func() {
		count := 1
		for {
			if err := kafkaClient.Producer("p2").WriteMessages(context.Background(), &ekafka.Message{
				Value: []byte("p2 hello world, count:" + strconv.Itoa(count)),
			}); err != nil {
				elog.Error("produce fail", elog.FieldErr(err))
			}
			count++
			time.Sleep(time.Second)
		}
	}()

	app := ego.New().Serve(
		// 可以搭配其他服务模块一起使用
		egovernor.Load("server.governor").Build(),

		// 初始化 Consumer Server cs1
		func() *ekafka.ConsumerServer {
			// 依赖 `ekafka` 管理 Kafka consumer
			cs := kafkaClient.ConsumerServer("cs1")
			// 注册处理消息的回调函数
			cs.OnConsumeEachMessage(func(ctx context.Context, message *kafka.Message) error {
				fmt.Printf("cs1 got a message: %s\n", string(message.Value))
				// 如果返回错误则会被转发给 `consumptionErrors`
				return nil
			})

			return cs
		}(),

		// 初始化 Consumer Server cs2
		func() *ekafka.ConsumerServer {
			// 依赖 `ekafka` 管理 Kafka consumer
			cs := kafkaClient.ConsumerServer("cs2")
			// 注册处理消息的回调函数
			cs.OnConsumeEachMessage(func(ctx context.Context, message *kafka.Message) error {
				fmt.Printf("cs2 got a message: %s\n", string(message.Value))
				// 如果返回错误则会被转发给 `consumptionErrors`
				return nil
			})

			return cs
		}(),
	)
	if err := app.Run(); err != nil {
		elog.Panic("startup", elog.Any("err", err))
	}
}
