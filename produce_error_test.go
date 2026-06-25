package ekafka

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/segmentio/kafka-go"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
	"github.com/stretchr/testify/require"
)

func TestNormalizeWriteError_PrefersRecordedKafkaErrorOverContextDeadline(t *testing.T) {
	recorder := newProduceErrorRecorder()
	recorder.set(kafka.NotEnoughReplicas)

	err := normalizeWriteError(context.DeadlineExceeded, recorder)
	require.ErrorIs(t, err, kafka.NotEnoughReplicas)
	require.Contains(t, err.Error(), "Not Enough Replicas")
}

func TestNormalizeWriteError_ReturnsContextErrorWhenNoKafkaErrorRecorded(t *testing.T) {
	recorder := newProduceErrorRecorder()

	err := normalizeWriteError(context.DeadlineExceeded, recorder)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestNormalizeWriteError_UnwrapsWriteErrors(t *testing.T) {
	writeErr := kafka.WriteErrors{
		kafka.NotEnoughReplicas,
	}
	err := normalizeWriteError(writeErr, nil)
	require.ErrorIs(t, err, kafka.NotEnoughReplicas)
}

func TestErrorCapturingTransport_RecordsProducePartitionError(t *testing.T) {
	recorder := newProduceErrorRecorder()
	transport := &errorCapturingTransport{
		base: roundTripFunc(func(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
			return &produceAPI.Response{
				Topics: []produceAPI.ResponseTopic{{
					Topic: "demo",
					Partitions: []produceAPI.ResponsePartition{{
						Partition: 0,
						ErrorCode: int16(kafka.NotEnoughReplicas),
					}},
				}},
			}, nil
		}),
		recorder: recorder,
	}

	_, err := transport.RoundTrip(context.Background(), nil, &produceAPI.Request{})
	require.NoError(t, err)
	require.ErrorIs(t, recorder.get(), kafka.NotEnoughReplicas)
}

type roundTripFunc func(context.Context, net.Addr, kafka.Request) (kafka.Response, error)

func (f roundTripFunc) RoundTrip(ctx context.Context, addr net.Addr, req kafka.Request) (kafka.Response, error) {
	return f(ctx, addr, req)
}

func TestIsContextDone(t *testing.T) {
	require.True(t, isContextDone(context.DeadlineExceeded))
	require.True(t, isContextDone(errors.Join(context.Canceled, errors.New("wrapped"))))
	require.False(t, isContextDone(kafka.NotEnoughReplicas))
}
