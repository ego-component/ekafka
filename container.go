package ekafka

import (
	"fmt"

	"github.com/gotomicro/ego/core/econf"
	"github.com/gotomicro/ego/core/elog"
)

type Option func(c *Container)

type Container struct {
	config *config
	name   string
	logger *elog.Component
}

// DefaultContainer 返回默认Container
func DefaultContainer() *Container {
	return &Container{
		config: DefaultConfig(),
		logger: elog.EgoLogger.With(elog.FieldComponent(PackageName)),
	}
}

// Load 载入配置，初始化Container
func Load(key string) *Container {
	c := DefaultContainer()
	if err := econf.UnmarshalKey(key, &c.config, econf.WithWeaklyTypedInput(true)); err != nil {
		c.logger.Panic("parse config error", elog.FieldErr(err), elog.FieldKey(key))
		return c
	}

	c.logger = c.logger.With(elog.FieldComponentName(key))
	c.name = key
	if c.config.EnableCompress {
		if c.config.CompressLimit <= 0 {
			c.config.CompressLimit = defaultCompressLimit
		}
		if c.config.CompressType == "" {
			c.config.CompressType = compressTypeGzip
		}
	}

	// 为没有显式设置 WatchPartitionChanges 的 consumer 配置应用默认值
	c.applyDefaultConsumerSettings()

	return c
}

// applyDefaultConsumerSettings 为 consumer 和 consumerGroup 配置应用默认设置
func (c *Container) applyDefaultConsumerSettings() {
	// 为 consumers 应用默认设置
	for name, consumerConf := range c.config.Consumers {
		// 如果 WatchPartitionChanges 为 nil（未设置），且该 consumer 配置有效，则设置为默认值 true
		if consumerConf.WatchPartitionChanges == nil && (consumerConf.Topic != "" || consumerConf.GroupID != "") {
			watchPartitionChanges := true
			consumerConf.WatchPartitionChanges = &watchPartitionChanges
			c.config.Consumers[name] = consumerConf
		}
	}

	// 为 consumerGroups 应用默认设置
	for name, consumerGroupConf := range c.config.ConsumerGroups {
		// 如果 WatchPartitionChanges 为 nil（未设置），且该 consumerGroup 配置有效，则设置为默认值 true
		if consumerGroupConf.WatchPartitionChanges == nil && (consumerGroupConf.Topic != "" || consumerGroupConf.GroupID != "") {
			watchPartitionChanges := true
			consumerGroupConf.WatchPartitionChanges = &watchPartitionChanges
			c.config.ConsumerGroups[name] = consumerGroupConf
		}
	}
}

// Build 构建Container
func (c *Container) Build(options ...Option) *Component {
	// 放第一个时间才准确
	options = append(options, WithClientInterceptor(fixedClientInterceptor(c.name, c.config)))
	options = append(options, WithClientInterceptor(traceClientInterceptor(c.name, c.config)))
	options = append(options, WithClientInterceptor(accessClientInterceptor(c.name, c.config, c.logger)))
	if c.config.EnableMetricInterceptor {
		options = append(options, WithClientInterceptor(metricClientInterceptor(c.name, c.config)))
	}
	if c.config.EnableCompress {
		options = append(options, WithClientInterceptor(compressClientInterceptor(c.name, c.config, c.logger)))
	}
	options = append(options, WithServerInterceptor(fixedServerInterceptor(c.name, c.config)))
	options = append(options, WithServerInterceptor(traceServerInterceptor(c.name, c.config)))
	options = append(options, WithServerInterceptor(accessServerInterceptor(c.name, c.config, c.logger)))
	if c.config.EnableMetricInterceptor {
		options = append(options, WithServerInterceptor(metricServerInterceptor(c.name, c.config)))
	}
	if c.config.EnableCompress {
		options = append(options, WithServerInterceptor(compressServerInterceptor(c.name, c.config, c.logger)))
	}

	for _, option := range options {
		option(c)
	}

	c.logger = c.logger.With(elog.FieldAddr(fmt.Sprintf("%s", c.config.Brokers)))
	cmp := &Component{
		config:          c.config,
		logger:          c.logger,
		producers:       make(map[string]*Producer),
		consumers:       make(map[string]*Consumer),
		consumerServers: make(map[string]*ConsumerServer),
		consumerGroups:  make(map[string]*ConsumerGroup),
		compName:        c.name,
	}

	return cmp
}
