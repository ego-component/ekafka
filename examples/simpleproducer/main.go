package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/etrace"
	"github.com/gotomicro/ego/core/etrace/ejaeger"

	"github.com/ego-component/ekafka"
)

// produce 生产消息
func main() {
	// 假设你配置的toml如下所示
	etrace.SetGlobalTracer(ejaeger.DefaultConfig().Build())
	conf := `
[kafka]
	debug=true
	brokers = ["192.168.64.65:9091", "192.168.64.65:9092", "192.168.64.65:9093"]
EnableAccessInterceptor = true
	[kafka.client]
		timeout="3s"
	[kafka.producers.p1]        # 定义了名字为 p1 的 producer
		topic="sre-infra-debug"  # 指定生产消息的 topic 
`
	// 加载配置文件
	err := econf.LoadFromReader(strings.NewReader(conf), toml.Unmarshal)
	if err != nil {
		panic("LoadFromReader fail," + err.Error())
	}

	// 初始化 ekafka 组件
	cmp := ekafka.Load("kafka").Build()

	// 使用 p1 生产者生产消息
	producerClient := cmp.Producer("p1")

	// 生产3条消息
	err = producerClient.WriteMessages(
		context.Background(),
		&ekafka.Message{Value: []byte("Hello World!")},
		&ekafka.Message{Value: []byte("One!")},
		&ekafka.Message{Value: []byte("Two!")},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := producerClient.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	fmt.Println(`produce message success --------------->`)
}
