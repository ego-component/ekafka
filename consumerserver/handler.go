package consumerserver

import (
	"context"
	"errors"

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

type listenerWrapper struct {
	onEachMessageHandler        OnEachMessageHandler
	onConsumeEachMessageHandler OnConsumeEachMessageHandler
}

func (w listenerWrapper) Handle(ctx context.Context, message *ekafka.Message) (bool, error) {
	if w.onEachMessageHandler != nil {
		err := w.onEachMessageHandler(ctx, *message)
		if err != nil {
			if errors.Is(err, ekafka.ErrDoNotCommit) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if w.onConsumeEachMessageHandler != nil {
		err := w.onConsumeEachMessageHandler(ctx, message)
		if err != nil {
			if errors.Is(err, ekafka.ErrDoNotCommit) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	panic("no handler wrapped")
}
