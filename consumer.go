package ntikafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-faster/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Cfg struct {
	host string
	user string
	pass string
	errh ErrorHandler
}

func Host(host string) Option {
	return func(cfg *Cfg) {
		cfg.host = host
	}
}

func Credentials(user, password string) Option {
	return func(cfg *Cfg) {
		cfg.user = user
		cfg.pass = password
	}
}

func WithErrorHandler(fn ErrorHandler) Option {
	return func(c *Cfg) {
		c.errh = fn
	}
}

type Option func(*Cfg)

type MessageHandler func(context.Context, kafka.Message) error

type ErrorHandler func(context.Context, error)

var tlsConfig *tls.Config

func init() {
	ca := os.Getenv("KAFKA_CA_PEM")
	if ca == "" {
		return
	}
	certs := x509.NewCertPool()
	certs.AppendCertsFromPEM([]byte(ca))
	tlsConfig = &tls.Config{
		InsecureSkipVerify: true,
		RootCAs:            certs,
	}
}

// Запуск Kafka Consumer в составе группы group и топика topic.
func Start(ctx context.Context, log *slog.Logger, topic, group string, fn MessageHandler, opts ...Option) (err error) {
	cfg := &Cfg{
		host: os.Getenv("KAFKA_HOST"),
		user: os.Getenv("KAFKA_USER"),
		pass: os.Getenv("KAFKA_PASS"),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	mechanism, err := scram.Mechanism(scram.SHA512, cfg.user, cfg.pass)
	if err != nil {
		return errors.Wrap(err, "scram.Mechanism")
	}
	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		Timeout:       time.Second * 10,
		DualStack:     true,
	}
	if tlsConfig != nil {
		dialer.TLS = tlsConfig
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Dialer:  dialer,
		Brokers: []string{cfg.host},
		Topic:   topic,
		GroupID: group,
		ErrorLogger: kafka.LoggerFunc(func(msg string, attrs ...any) {
			log.ErrorContext(ctx, fmt.Sprintf(msg, attrs...))
		}),
	})
	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	handler := &handler{
		tracer: otel.GetTracerProvider().Tracer("ntikafka"),
		errh:   cfg.errh,
		impl:   fn,
	}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := handler.handle(ctx, msg); err != nil {
			return err
		}
		if err = reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

type handler struct {
	tracer trace.Tracer
	errh   ErrorHandler
	impl   MessageHandler
}

var (
	attrMsgBytes = attribute.Key("message.value.bytes")
)

func (h *handler) handle(ctx context.Context, msg kafka.Message) error {
	ctx, span := h.tracer.Start(
		ctx, "kconsumer.handle", trace.WithAttributes(
			attrMsgBytes.Int(len(msg.Value)),
		),
	)
	defer span.End()
	err := h.impl(ctx, msg)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			span.AddEvent("отменена обработка сообщения")
			return err
		}
		span.SetStatus(codes.Error, "обработчик сообщения вернул ошибку")
		if h.errh != nil {
			h.errh(ctx, err)
		}
		return err
	}
	span.SetStatus(codes.Ok, "обработка сообщения успешно завершена")
	return nil
}
