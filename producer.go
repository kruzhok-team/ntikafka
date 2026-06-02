package ntikafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

func Producer(ctx context.Context, netErrLog *slog.Logger) (p *kafka.Writer, err error) {
	cfg, err := newConnectCfg()
	if err != nil {
		return p, err
	}
	mechanism, err := cfg.mechanism()
	if err != nil {
		return p, err
	}
	transport := &kafka.Transport{
		SASL:     mechanism,
		ClientID: clientID(ctx),
	}
	if tlsConfig != nil {
		transport.TLS = tlsConfig
	}
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.host),
		Topic:        cfg.topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Transport:    transport,
		ErrorLogger: kafka.LoggerFunc(func(msg string, attrs ...any) {
			netErrLog.ErrorContext(ctx, fmt.Sprintf(msg, attrs...))
		}),
	}, nil
}
