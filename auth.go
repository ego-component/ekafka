package ekafka

import (
	"crypto/tls"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Authentication struct {
	// TLS authentication
	TLS *TLSConfig
}

func (config *Authentication) ConfigureTransportAuthentication(opts *kafka.Transport) (err error) {
	if config.TLS != nil {
		if opts.TLS, err = configureTLS(config.TLS); err != nil {
			return err
		}
	}
	return nil
}

func (config *Authentication) ConfigureDialerAuthentication(opts *kafka.Dialer) (err error) {
	if config.TLS != nil {
		if opts.TLS, err = configureTLS(config.TLS); err != nil {
			return err
		}
	}
	return nil
}

func configureTLS(config *TLSConfig) (*tls.Config, error) {
	tlsConfig, err := config.LoadTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("error loading tls config: %w", err)
	}
	if tlsConfig != nil && tlsConfig.InsecureSkipVerify {
		return tlsConfig, nil
	}
	return nil, nil
}
