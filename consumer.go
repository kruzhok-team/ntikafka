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
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

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

type config struct {
	host  string
	user  string
	pass  string
	topic string
	group string
}

const dialerTimeout time.Duration = time.Second * 10

// Обработчик сообщения.
type MessageHandler func(context.Context, kafka.Message) error

// // Запуск Kafka Consumer в составе группы group и топика topic.
func Consume(ctx context.Context, netErrLog *slog.Logger, handler MessageHandler) (err error) {
	cfg := config{}
	for key, prop := range map[string]*string{
		"KAFKA_HOST":  &cfg.host,
		"KAFKA_USER":  &cfg.user,
		"KAFKA_PASS":  &cfg.pass,
		"KAFKA_TOPIC": &cfg.topic,
		"KAFKA_GROUP": &cfg.group,
	} {
		v := os.Getenv(key)
		if v == "" {
			return fmt.Errorf("отсутствует значение переменной окружения %s", key)
		}
		*prop = v
	}
	var mechanism sasl.Mechanism
	if cfg.user != "" {
		mechanism, err = scram.Mechanism(scram.SHA512, cfg.user, cfg.pass)
		if err != nil {
			return errors.Wrap(err, "scram.Mechanism")
		}
	}
	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		Timeout:       dialerTimeout,
		DualStack:     true,
	}
	if tlsConfig != nil {
		dialer.TLS = tlsConfig
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Dialer:  dialer,
		Brokers: []string{cfg.host},
		Topic:   cfg.topic,
		GroupID: cfg.group,
		ErrorLogger: kafka.LoggerFunc(func(msg string, attrs ...any) {
			netErrLog.ErrorContext(ctx, fmt.Sprintf(msg, attrs...))
		}),
	})
	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	tracer := otel.GetTracerProvider().Tracer("ntikafka")
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := consumeMessage(ctx, tracer, msg, handler); err != nil {
			return err
		}
		if err = reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

const attrMsgBytes = attribute.Key("message.value.bytes")

// Враппер для handler дополняющий его спаном телеметрии.
func consumeMessage(ctx context.Context, tracer trace.Tracer, msg kafka.Message, handler MessageHandler) error {
	ctx, span := tracer.Start(
		ctx, "consumeMessage", trace.WithAttributes(
			attrMsgBytes.Int(len(msg.Value)),
		),
	)
	defer span.End()
	err := handler(ctx, msg)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			span.AddEvent("отменена обработка сообщения")
			return err
		}
		span.SetStatus(codes.Error, "обработчик сообщения вернул ошибку")
		span.RecordError(err)
		return err
	}
	span.SetStatus(codes.Ok, "обработка сообщения успешно завершена")
	return nil
}
