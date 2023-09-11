package main

import (
	"context"
	"fmt"
	"github.com/ego-component/ekafka"
	"github.com/gotomicro/ego"
	"log"
)

func main() {
	ego.New().Invoker(func() error {
		//初始化ekafka组件
		cmp := ekafka.Load("kafka").Build()
		// 使用p1生产者生产消息
		produce(context.Background(), cmp.Producer("p1"))

		// 使用c1消费者消费消息
		consume(cmp.Consumer("c1"))
		return nil
	}).Run()

}

func produce(ctx context.Context, w *ekafka.Producer) {
	err := w.WriteMessages(ctx, &ekafka.Message{
		Key:   []byte("Key-compress"),
		Value: []byte("Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!"),
	})
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	err = w.WriteMessages(ctx, &ekafka.Message{
		Key:   []byte("Key-no-compress"),
		Value: []byte("Hello World!"),
	})
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	if err = w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

// consume 使用consumer/consumerGroup消费消息
func consume(r *ekafka.Consumer) {
	ctx := context.Background()
	err := r.SetOffset(32)
	if err != nil {
		panic(err)
	}
	for {
		// ReadMessage 再收到下一个Message时，会阻塞
		msg, ctxOutput, err := r.FetchMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// 打印消息
		fmt.Println("received headers: ", msg.Headers)
		fmt.Println("received: ", string(msg.Value))
		err = r.CommitMessages(ctxOutput, &msg)
		if err != nil {
			log.Printf("fail to commit msg:%v", err)
		}
	}
}
