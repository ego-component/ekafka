package consumerserver

import (
	"context"

	"github.com/ego-component/ekafka"
	"github.com/segmentio/kafka-go"
)

// OnEachMessageHandler ...
type OnEachMessageHandler = func(ctx context.Context, message kafka.Message) error

// OnConsumeEachMessageHandler ...
type OnConsumeEachMessageHandler = func(ctx context.Context, message *ekafka.Message) error

// OnStartHandler ...
type OnStartHandler = func(ctx context.Context, consumer *ekafka.Consumer) error

// OnConsumerGroupStartHandler ...
type OnConsumerGroupStartHandler = func(ctx context.Context, consumerGroup *ekafka.ConsumerGroup) error
