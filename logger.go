package ekafka

import (
	"strings"

	"github.com/gotomicro/ego/core/elog"
)

// errorLogger is an elog to kafka-go Logger adapter
type logger struct {
	*elog.Component
}

func (l *logger) Printf(tmpl string, args ...interface{}) {
	// 正确的信息，并且太多，过滤掉
	if strings.HasPrefix(tmpl, "no messages received from kafka") {
		return
	}
	l.Infof(tmpl, args...)
}

// errorLogger is an elog to kafka-go ErrorLogger adapter
type errorLogger struct {
	*elog.Component
}

func (l *errorLogger) Printf(tmpl string, args ...interface{}) {
	l.Errorf(tmpl, args...)
}

func newKafkaLogger(wrappedLogger *elog.Component) *logger {
	return &logger{wrappedLogger.With(elog.FieldMethod("internal"))}
}

func newKafkaErrorLogger(wrappedLogger *elog.Component) *errorLogger {
	return &errorLogger{wrappedLogger.With(elog.FieldMethod("internal"))}
}
