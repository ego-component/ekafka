package consumerserver

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/gotomicro/ego/core/elog"
	"go.uber.org/zap"

	"github.com/ego-component/ekafka"
)

type Handler func(ctx context.Context, message *ekafka.Message) error
type BatchHandler func(ctx context.Context, messages []*ekafka.Message) error

type Listener interface {
	Handle(ctx context.Context, message *ekafka.Message) (bool, error)
}

type listeners []Listener

func (l listeners) Dispatch(ctx context.Context, message *ekafka.Message) (err error) {
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			elog.Error("kafka_handle_panic", zap.String("stack", string(buf[:n])))
		}
	}()

	var errs consumerErrors
	commitCount := 0
	// TODO 每个 listener 是否应该并发运行？
	for _, listener := range l {
		if listener == nil {
			commitCount++
			continue
		}
		retryCount := 0
	HANDLER:
		commitOffset, err := listener.Handle(ctx, message)
		if err != nil {
			// If it's a retryable error, we should execute the handler again.
			if errors.Is(err, ekafka.ErrRecoverableError) && retryCount < maxOnEachMessageHandlerRetryCount {
				retryCount++
				goto HANDLER
			}
			errs = append(errs, err)
			elog.Error("kafka consumer handle error", elog.FieldErr(err), elog.String("tag", "kafka_consumer"), elog.String("topic", message.Topic), elog.String("partition", fmt.Sprintf("%d", message.Partition)), elog.String("offset", fmt.Sprintf("%d", message.Offset)))
		}
		if commitOffset {
			commitCount++
		}
	}
	if commitCount != len(l) {
		if len(errs) > 0 {
			return fmt.Errorf("commitCount != len(listeners), %w", errs)
		}
		return ekafka.ErrDoNotCommit
	}
	return nil
}

type consumerErrors []error

func (e consumerErrors) Error() string {
	var s string
	for _, err := range e {
		s += err.Error() + "\n"
	}
	return s
}

type SyncListener struct {
	Handler Handler
}

func NewListener(handler Handler) Listener {
	return &SyncListener{
		Handler: handler,
	}
}

func (l *SyncListener) Handle(ctx context.Context, message *ekafka.Message) (bool, error) {
	if l.Handler != nil {
		if err := l.Handler(ctx, message); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, fmt.Errorf("handler not set")
}

type BatchListener struct {
	Batch           []*ekafka.Message
	BatchUpdateSize int
	Timeout         time.Duration
	Handler         BatchHandler
}

func NewBatchListener(handler BatchHandler, batchUpdateSize int, timeout time.Duration) Listener {
	return &BatchListener{
		Handler:         handler,
		Batch:           make([]*ekafka.Message, 0, batchUpdateSize),
		BatchUpdateSize: batchUpdateSize,
		Timeout:         timeout,
	}
}

func (l *BatchListener) Handle(ctx context.Context, message *ekafka.Message) (bool, error) {
	l.Batch = append(l.Batch, message)
	elog.Info("kafka_consumer_batch", elog.Any("batch_len", len(l.Batch)), elog.Any("time_since", time.Since(l.Batch[0].Time)))

	var err error
	var storeOffset bool
	if l.BatchUpdateSize > 0 && len(l.Batch) >= l.BatchUpdateSize {
		if err = l.Handler(ctx, l.Batch[:l.BatchUpdateSize]); err != nil {
			elog.Error("batch_handle_message_fail", zap.Int("batch_len", len(l.Batch)))
			return false, err
		}
		copy(l.Batch, l.Batch[l.BatchUpdateSize:])
		l.Batch = l.Batch[:len(l.Batch)-l.BatchUpdateSize]
		storeOffset = true
	} else if len(l.Batch) > 0 && time.Since(l.Batch[0].Time) >= l.Timeout {
		if err = l.Handler(ctx, l.Batch); err != nil {
			elog.Error("batch_handle_message_fail", zap.Int("batch_len", len(l.Batch)), zap.Int64("time", l.Batch[0].Time.Unix()))
			return false, err
		}

		l.Batch = l.Batch[0:0]
		storeOffset = true
	}

	return storeOffset, err
}