package consumerserver

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/gotomicro/ego/core/elog"
	"go.uber.org/zap"

	"github.com/ego-component/ekafka"
)

type Handler func(ctx context.Context, message *ekafka.Message) error
type BatchHandler func(lastCtx context.Context, messages []*ekafka.CtxMessage) error

type Listener interface {
	Handle(ctx context.Context, message *ekafka.Message, opts ...handleOption) (bool, error)
}

type ctxKeyInBatch struct{}

type listeners []Listener

func (l listeners) dispatch(ctx context.Context, message *ekafka.Message, logger *elog.Component) (err error) {
	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			logger.Error("kafka_handle_panic", elog.FieldCtxTid(ctx), zap.String("stack", string(buf[:n])))
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
			logger.Error("kafka consumer handle error", elog.FieldCtxTid(ctx), elog.FieldErr(err), elog.String("tag", "kafka_consumer"), elog.String("topic", message.Topic), elog.String("partition", fmt.Sprintf("%d", message.Partition)), elog.String("offset", fmt.Sprintf("%d", message.Offset)))
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
	logger  *elog.Component
}

func (cmp *Component) newListener(handler Handler) Listener {
	return &SyncListener{
		Handler: handler,
		logger:  cmp.logger,
	}
}

func (l *SyncListener) Handle(ctx context.Context, message *ekafka.Message, opts ...handleOption) (bool, error) {
	if l.Handler != nil {
		if err := l.Handler(ctx, message); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, fmt.Errorf("handler not set")
}

type BatchListener struct {
	mtx             sync.RWMutex
	Batch           []*ekafka.CtxMessage
	BatchUpdateSize int
	Timeout         time.Duration
	Handler         BatchHandler
	logger          *elog.Component
	consumer        *ekafka.Consumer
}

func (cmp *Component) newBatchListener(handler BatchHandler, batchUpdateSize int, timeout time.Duration) Listener {
	bl := &BatchListener{
		Handler:         handler,
		Batch:           make([]*ekafka.CtxMessage, 0, batchUpdateSize),
		BatchUpdateSize: batchUpdateSize,
		Timeout:         timeout,
		logger:          cmp.logger,
		consumer:        cmp.Consumer(),
	}
	go func() {
		// TODO set duration from config
		t := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-cmp.ServerCtx.Done():
				bl.logger.Info("ServerCtx.Done")
				return
			case <-t.C:
				_, err := bl.Handle(context.Background(), nil, withIntervalCommitSig(true))
				if err != nil && !errors.Is(err, ekafka.ErrDoNotCommit) {
					bl.logger.Error("newBatchListener timer handle fail", elog.FieldErr(err))
					break
				}
			}
		}
	}()
	return bl
}

type handleOpt struct {
	intervalCommitSig bool
}

type handleOption func(c *handleOpt)

func withIntervalCommitSig(intervalCommitSig bool) handleOption {
	return func(c *handleOpt) {
		c.intervalCommitSig = intervalCommitSig
	}
}

func (l *BatchListener) Handle(ctx context.Context, message *ekafka.Message, optFuncs ...handleOption) (bool, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	var o = &handleOpt{}
	for _, optFunc := range optFuncs {
		optFunc(o)
	}

	if message != nil {
		l.Batch = append(l.Batch, &ekafka.CtxMessage{
			Message: message,
			Ctx:     context.WithValue(ctx, ctxKeyInBatch{}, time.Now()),
		})
		l.logger.Debug("kafka_consumer_batch", elog.FieldCtxTid(ctx), elog.Int("batch_len", len(l.Batch)), elog.Duration("time_since", time.Since(l.Batch[0].Time)))
	}

	var err error
	var storeOffset bool
	if l.BatchUpdateSize > 0 && len(l.Batch) >= l.BatchUpdateSize {
		if err = l.Handler(ctx, l.Batch[:l.BatchUpdateSize]); err != nil {
			l.logger.Error("batch_handle_message_fail", elog.FieldCtxTid(ctx), zap.Int("batch_len", len(l.Batch)))
			return false, err
		}
		copy(l.Batch, l.Batch[l.BatchUpdateSize:])
		l.Batch = l.Batch[:len(l.Batch)-l.BatchUpdateSize]
		storeOffset = true
	} else if len(l.Batch) > 0 && time.Since(l.Batch[0].Ctx.Value(ctxKeyInBatch{}).(time.Time)) >= l.Timeout {
		if err = l.Handler(ctx, l.Batch); err != nil {
			l.logger.Error("batch_handle_message_fail", elog.FieldCtxTid(ctx), zap.Int("batch_len", len(l.Batch)), zap.Int64("time", l.Batch[0].Time.Unix()))
			return false, err
		}

		// 需要周期性提交还在内存中的message
		if o.intervalCommitSig {
			l.logger.Debug("kafka_consumer_batch timer try to commit message in memory")
			if err := l.consumer.CommitMessages(ctx, l.Batch[len(l.Batch)-1].Message); err != nil {
				l.logger.Error("newBatchListener timer handle CommitMessages fail", elog.FieldErr(err))
			}
		}
		l.Batch = l.Batch[0:0]
		storeOffset = true
	}

	return storeOffset, err
}
