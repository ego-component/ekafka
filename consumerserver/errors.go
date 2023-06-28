package consumerserver

import "errors"

var (
	ErrRecoverableError = errors.New("recoverable error is retryable")
	ErrDoNotCommit      = errors.New("do not commit")
)
