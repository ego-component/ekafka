package consumerserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gotomicro/ego/core/constant"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/core/emetric"
	"github.com/gotomicro/ego/server"
	"github.com/segmentio/kafka-go"

	"github.com/ego-component/ekafka"
)

// OnEachMessageHandler 的最大重试次数
const maxOnEachMessageHandlerRetryCount = 3

// Interface check
var _ server.Server = (*Component)(nil)

// PackageName is the name of this component.
const PackageName = "component.ekafka.consumerserver"

type consumptionMode int

const (
	consumptionModeOnConsumerStart consumptionMode = iota + 1
	consumptionModeOnConsumerEachMessage
	consumptionModeOnConsumerGroupStart
	consumptionModeOnConsumerConsumeEachMessage
)

// Component starts an Ego server for message consuming.
type Component struct {
	ServerCtx                   context.Context
	stopServer                  context.CancelFunc
	config                      *config
	name                        string
	ekafkaComponent             *ekafka.Component
	logger                      *elog.Component
	mode                        consumptionMode
	onConsumerStartHandler      OnStartHandler
	onConsumerGroupStartHandler OnConsumerGroupStartHandler
	listeners                   listeners
	consumptionErrors           chan<- error
}

// PackageName returns the package name.
func (cmp *Component) PackageName() string {
	return PackageName
}

// Info returns server info, used by governor and consumer balancer.
func (cmp *Component) Info() *server.ServiceInfo {
	info := server.ApplyOptions(
		server.WithKind(constant.ServiceProvider),
	)
	return &info
}

// GracefulStop stops the server.
func (cmp *Component) GracefulStop(ctx context.Context) error {
	cmp.stopServer()
	return nil
}

// Stop stops the server.
func (cmp *Component) Stop() error {
	cmp.stopServer()
	return nil
}

// Init ...
func (cmp *Component) Init() error {
	return nil
}

// Name returns the name of this instance.
func (cmp *Component) Name() string {
	return cmp.name
}

// Start will start consuming.
func (cmp *Component) Start() error {
	switch cmp.mode {
	case consumptionModeOnConsumerStart:
		return cmp.launchOnConsumerStart()
	case consumptionModeOnConsumerGroupStart:
		return cmp.launchOnConsumerGroupStart()
	case consumptionModeOnConsumerEachMessage:
		return cmp.launchOnConsumerEachMessage()
	case consumptionModeOnConsumerConsumeEachMessage:
		return cmp.launchOnConsumerConsumeEachMessage()
	default:
		return fmt.Errorf("undefined consumption mode: %v", cmp.mode)
	}
}

// Consumer returns the default Consumer.
func (cmp *Component) Consumer() *ekafka.Consumer {
	return cmp.ekafkaComponent.Consumer(cmp.config.ConsumerName)
}

// ConsumerGroup returns the default ConsumerGroup.
func (cmp *Component) ConsumerGroup() *ekafka.ConsumerGroup {
	return cmp.ekafkaComponent.ConsumerGroup(cmp.config.ConsumerGroupName)
}

// OnEachMessage ...
// Deprecated: use OnConsumeEachMessage instead.
func (cmp *Component) OnEachMessage(consumptionErrors chan<- error, handler OnEachMessageHandler) error {
	cmp.consumptionErrors = consumptionErrors
	cmp.mode = consumptionModeOnConsumerEachMessage
	cmp.listeners = listeners{listenerWrapper{onEachMessageHandler: handler}}
	return nil
}

// OnConsumeEachMessage register a handler for each message. When the handler returns an error, the message will be
// retried if the error is ErrRecoverableError else the message will not be committed.
func (cmp *Component) OnConsumeEachMessage(handler OnConsumeEachMessageHandler) error {
	cmp.mode = consumptionModeOnConsumerConsumeEachMessage
	cmp.listeners = listeners{listenerWrapper{onConsumeEachMessageHandler: handler}}
	return nil
}

// Subscribe append a handler for each message.
func (cmp *Component) Subscribe(listener Listener) {
	cmp.mode = consumptionModeOnConsumerConsumeEachMessage
	cmp.listeners = append(cmp.listeners, listener)
}

// SubscribeSingleHandler append a single listener with this handler for each message
func (cmp *Component) SubscribeSingleHandler(handler Handler) {
	cmp.mode = consumptionModeOnConsumerConsumeEachMessage
	cmp.listeners = append(cmp.listeners, cmp.NewListener(handler))
}

// SubscribeBatchHandler append a batch listener with this handler for each message. A batch messages will be handled when
// batch size or timeout reached
func (cmp *Component) SubscribeBatchHandler(handler BatchHandler, batchSize int, timeout time.Duration) {
	cmp.mode = consumptionModeOnConsumerConsumeEachMessage
	cmp.listeners = append(cmp.listeners, cmp.NewBatchListener(handler, batchSize, timeout))
}

// OnStart ...
func (cmp *Component) OnStart(handler OnStartHandler) error {
	cmp.mode = consumptionModeOnConsumerStart
	cmp.onConsumerStartHandler = handler
	return nil
}

// OnConsumerGroupStart ...
func (cmp *Component) OnConsumerGroupStart(handler OnConsumerGroupStartHandler) error {
	cmp.mode = consumptionModeOnConsumerGroupStart
	cmp.onConsumerGroupStartHandler = handler
	return nil
}

func isErrorUnrecoverable(err error) bool {
	if kafkaError, ok := err.(kafka.Error); ok {
		if kafkaError.Temporary() {
			return false
		}
	}
	return true
}

func (cmp *Component) launchOnConsumerGroupStart() error {
	consumerGroup := cmp.ConsumerGroup()

	if cmp.onConsumerGroupStartHandler == nil {
		return errors.New("you must define a MessageHandler first")
	}

	handlerExit := make(chan error)
	go func() {
		handlerExit <- cmp.onConsumerGroupStartHandler(cmp.ServerCtx, consumerGroup)
		close(handlerExit)
	}()

	var originErr error
	select {
	case originErr = <-handlerExit:
		if originErr != nil {
			cmp.logger.Error("terminating ConsumerGroup because an error", elog.FieldErr(originErr))
		} else {
			cmp.logger.Info("message handler exited without any error, terminating ConsumerGroup")
		}
		cmp.stopServer()
	case <-cmp.ServerCtx.Done():
		originErr = cmp.ServerCtx.Err()
		cmp.logger.Error("terminating ConsumerGroup because a context error", elog.FieldErr(originErr))

		err := <-handlerExit
		if err != nil {
			cmp.logger.Error("terminating ConsumerGroup because an error", elog.FieldErr(err))
		} else {
			cmp.logger.Info("message handler exited without any error")
		}
	}

	err := cmp.closeConsumerGroup(consumerGroup)
	if err != nil {
		return fmt.Errorf("encountered an error while closing ConsumerGroup: %w", err)
	}

	if errors.Is(originErr, context.Canceled) {
		return nil
	}

	return originErr
}

func (cmp *Component) launchOnConsumerStart() error {
	consumer := cmp.Consumer()

	if cmp.onConsumerStartHandler == nil {
		return errors.New("you must define a MessageHandler first")
	}

	handlerExit := make(chan error)
	go func() {
		handlerExit <- cmp.onConsumerStartHandler(cmp.ServerCtx, consumer)
		close(handlerExit)
	}()

	var originErr error
	select {
	case originErr = <-handlerExit:
		if originErr != nil {
			cmp.logger.Error("terminating ConsumerServer because an error", elog.FieldErr(originErr))
		} else {
			cmp.logger.Info("message handler exited without any error, terminating ConsumerServer")
		}
		cmp.stopServer()
	case <-cmp.ServerCtx.Done():
		originErr = cmp.ServerCtx.Err()
		cmp.logger.Error("terminating ConsumerServer because a context error", elog.FieldErr(originErr))

		err := <-handlerExit
		if err != nil {
			cmp.logger.Error("terminating ConsumerServer because an error", elog.FieldErr(err))
		} else {
			cmp.logger.Info("message handler exited without any error")
		}
	}

	err := cmp.closeConsumer(consumer)
	if err != nil {
		return fmt.Errorf("encountered an error while closing Consumer: %w", err)
	}

	if errors.Is(originErr, context.Canceled) {
		return nil
	}

	return originErr
}

func (cmp *Component) launchOnConsumerEachMessage() error {
	consumer := cmp.Consumer()
	if len(cmp.listeners) == 0 {
		return errors.New("you must define a MessageHandler first")
	}

	compNameTopic := fmt.Sprintf("%s.%s", cmp.ekafkaComponent.GetCompName(), consumer.Config.Topic)
	brokers := strings.Join(consumer.Brokers, ",")
	go func() {
		for {
			if cmp.ServerCtx.Err() != nil {
				return
			}
			// The beginning of time monitoring point in time
			now := time.Now()
			message, fetchCtx, err := consumer.FetchMessage(cmp.ServerCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					cmp.logger.Info("consumerServer is terminating...")
					return
				}
				cmp.consumptionErrors <- err
				cmp.logger.Error("encountered an error while fetching message", elog.FieldErr(err))

				// try to fetch message again.
				continue
			}
			msgId := fmt.Sprintf("%s_%d_%d", consumer.Config.Topic, message.Partition, message.Offset)

			err = cmp.listeners.dispatch(fetchCtx, &message, cmp.logger)
			cmp.PackageName()
			// Record the redis time-consuming
			emetric.ClientHandleHistogram.WithLabelValues("kafka", compNameTopic, "HANDLER", brokers).Observe(time.Since(now).Seconds())
			if err != nil {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "HANDLER", brokers, "Error")
			} else {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "HANDLER", brokers, "OK")
			}

			if err != nil {
				cmp.logger.Error("encountered an error while handling message", elog.FieldErr(err), elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))
				cmp.consumptionErrors <- err

				continue
			}

		COMMIT:
			err = consumer.CommitMessages(fetchCtx, &message)

			// Record the kafka time-consuming
			emetric.ClientHandleHistogram.WithLabelValues("kafka", compNameTopic, "COMMIT", brokers).Observe(time.Since(now).Seconds())
			if err != nil {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "COMMIT", brokers, "Error")
			} else {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "COMMIT", brokers, "OK")
			}

			if err != nil {
				if errors.Is(err, context.Canceled) {
					cmp.logger.Info("consumerServer is terminating... will retry to commit message using background context")
					fetchCtx = context.Background()
					goto COMMIT
				}
				if errors.Is(err, io.ErrClosedPipe) && cmp.ServerCtx.Err() != nil {
					return
				}
				cmp.consumptionErrors <- err
				cmp.logger.Error("encountered an error while committing message", elog.FieldErr(err), elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))

				// Try to commit this message again.
				cmp.logger.Debug("try to commit message again", elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))
				goto COMMIT
			}
		}
	}()

	select {
	case <-cmp.ServerCtx.Done():
		rootErr := cmp.ServerCtx.Err()
		cmp.logger.Info("terminating consumer because a context error", elog.FieldErr(rootErr))

		err := cmp.closeConsumer(consumer)
		if err != nil {
			return fmt.Errorf("encountered an error while closing consumer: %w", err)
		}

		if errors.Is(rootErr, context.Canceled) {
			return nil
		}

		return rootErr
	}
}

func (cmp *Component) launchOnConsumerConsumeEachMessage() error {
	consumer := cmp.Consumer()
	if len(cmp.listeners) == 0 {
		return errors.New("you must define a MessageHandler first")
	}

	var (
		compNameTopic = fmt.Sprintf("%s.%s", cmp.ekafkaComponent.GetCompName(), consumer.Config.Topic)
		brokers       = strings.Join(consumer.Brokers, ",")
	)

	go func() {
		for {
			if cmp.ServerCtx.Err() != nil {
				return
			}
			// The beginning of time monitoring point in time
			now := time.Now()
			message, fetchCtx, err := consumer.FetchMessage(cmp.ServerCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					cmp.logger.Info("consumerServer is terminating...")
					return
				}
				cmp.logger.Error("encountered an error while fetching message", elog.FieldErr(err))

				// try to fetch message again.
				continue
			}
			msgId := fmt.Sprintf("%s_%d_%d", consumer.Config.Topic, message.Partition, message.Offset)

			err = cmp.listeners.dispatch(fetchCtx, &message, cmp.logger)
			// Record the redis kafka-consuming
			emetric.ClientHandleHistogram.WithLabelValues("kafka", compNameTopic, "HANDLER", brokers).Observe(time.Since(now).Seconds())
			if err != nil && !errors.Is(err, ekafka.ErrDoNotCommit) {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "HANDLER", brokers, "Error")
			} else {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "HANDLER", brokers, "OK")
			}

			if err != nil {
				if errors.Is(err, ekafka.ErrDoNotCommit) {
					cmp.logger.Debug("skipping commit message due to NotCommit error", elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))
					continue
				}

				// Otherwise should be considered as skipping commit message.
				cmp.logger.Error("skipping commit message due to an error", elog.FieldErr(err), elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))
				continue
			}
		COMMIT:
			err = consumer.CommitMessages(fetchCtx, &message)

			// Record the redis kafka-consuming
			emetric.ClientHandleHistogram.WithLabelValues("kafka", compNameTopic, "COMMIT", brokers).Observe(time.Since(now).Seconds())
			if err != nil {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "COMMIT", brokers, "Error")
			} else {
				emetric.ClientHandleCounter.Inc("kafka", compNameTopic, "COMMIT", brokers, "OK")
			}

			if err != nil {
				if errors.Is(err, context.Canceled) {
					cmp.logger.Info("consumerServer is terminating... will retry to commit message using background context")
					fetchCtx = context.Background()
					goto COMMIT
				}
				if errors.Is(err, io.ErrClosedPipe) && cmp.ServerCtx.Err() != nil {
					return
				}
				cmp.logger.Error("encountered an error while committing message", elog.FieldErr(err), elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))

				// Try to commit this message again.
				cmp.logger.Debug("try to commit message again", elog.FieldCtxTid(fetchCtx), elog.String("msgId", msgId))
				goto COMMIT
			}
		}
	}()

	select {
	case <-cmp.ServerCtx.Done():
		rootErr := cmp.ServerCtx.Err()
		cmp.logger.Info("terminating consumer because a context error", elog.FieldErr(rootErr))

		err := cmp.closeConsumer(consumer)
		if err != nil {
			return fmt.Errorf("encountered an error while closing consumer: %w", err)
		}

		if errors.Is(rootErr, context.Canceled) {
			return nil
		}

		return rootErr
	}
}

func (cmp *Component) closeConsumer(consumer *ekafka.Consumer) error {
	if err := consumer.Close(); err != nil {
		cmp.logger.Fatal("failed to close Consumer", elog.FieldErr(err))
		return err
	}
	cmp.logger.Info("Consumer closed")
	return nil
}

func (cmp *Component) closeConsumerGroup(consumerGroup *ekafka.ConsumerGroup) error {
	if err := consumerGroup.Close(); err != nil {
		cmp.logger.Fatal("failed to close ConsumerGroup", elog.FieldErr(err))
		return err
	}
	cmp.logger.Info("ConsumerGroup closed")
	return nil
}

// NewConsumerServerComponent creates a new server instance.
func NewConsumerServerComponent(name string, config *config, ekafkaComponent *ekafka.Component, logger *elog.Component) *Component {
	serverCtx, stopServer := context.WithCancel(context.Background())
	return &Component{
		ServerCtx:       serverCtx,
		stopServer:      stopServer,
		name:            name,
		config:          config,
		ekafkaComponent: ekafkaComponent,
		logger:          logger,
		mode:            consumptionModeOnConsumerEachMessage,
	}
}
