package ekafka

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/segmentio/kafka-go"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

// produceErrorRecorder keeps the latest Kafka broker error observed during produce
// requests. kafka-go may retry temporary errors and return context.DeadlineExceeded
// when the caller context expires first, which hides errors like NOT_ENOUGH_REPLICAS.
type produceErrorRecorder struct {
	mu  sync.Mutex
	err error
}

func newProduceErrorRecorder() *produceErrorRecorder {
	return &produceErrorRecorder{}
}

func (r *produceErrorRecorder) reset() {
	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()
}

func (r *produceErrorRecorder) set(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
}

func (r *produceErrorRecorder) get() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

type errorCapturingTransport struct {
	base     kafka.RoundTripper
	recorder *produceErrorRecorder
}

func newErrorCapturingTransport(base kafka.RoundTripper, recorder *produceErrorRecorder) kafka.RoundTripper {
	if base == nil || recorder == nil {
		return base
	}
	return &errorCapturingTransport{base: base, recorder: recorder}
}

func (t *errorCapturingTransport) RoundTrip(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
	res, err := t.base.RoundTrip(ctx, addr, req)
	t.captureResponse(res)
	if err != nil {
		t.recorder.set(err)
	}
	return res, err
}

func (t *errorCapturingTransport) captureResponse(res kafka.Response) {
	if res == nil {
		return
	}
	pr, ok := res.(*produceAPI.Response)
	if !ok {
		return
	}
	for _, topic := range pr.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				t.recorder.set(kafka.Error(partition.ErrorCode))
				return
			}
		}
	}
}

func normalizeWriteError(err error, recorder *produceErrorRecorder) error {
	if err == nil {
		return nil
	}

	if isContextDone(err) && recorder != nil {
		if kafkaErr := recorder.get(); kafkaErr != nil {
			return kafkaErr
		}
	}

	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		for _, item := range writeErrs {
			if item != nil {
				return item
			}
		}
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr
	}

	return err
}

func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
