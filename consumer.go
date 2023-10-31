package ntikafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var dialer *kafka.Dialer

type consumecfg struct {
	errHandler func(context.Context, error)
}

type ConsumeOpt func(*consumecfg)

func WithErrorHandler(fn func(context.Context, error)) ConsumeOpt {
	return func(ccfg *consumecfg) {
		ccfg.errHandler = fn
	}
}

func panicErrHandler(ctx context.Context, err error) {
	panic(err)
}

func SetDialer() error {
	mechanism, err := scram.Mechanism(
		scram.SHA512,
		"internal_consumer",
		os.Getenv("KAFKA_INTERNAL_CONSUMER_PASSWORD"),
	)
	if err != nil {
		return fmt.Errorf("scram.Mechanism: %w", err)
	}
	dialer = &kafka.Dialer{
		TLS:           tlscfg,
		SASLMechanism: mechanism,
		// ClientID:      clientID, TODO
		Timeout:   time.Second * 10,
		DualStack: true,
	}
	return nil
}

// Consume циклично читает и обрабатывает сообщения из топика в рамках Consumer Group.
//
// Контекст ctx используется для управления жизненным циклом.
//
// Прочитанное сообщение передается в handler, и если вызов завершится с nil ошибкой,
// то косьюмер коммитит оффест этого сообщения.
//
// Используется глобальный объект *kafka.Dialer. Если он отсуствует, то Consume
// попытается создать новый сразу же на запуске, и если в процессе возникнет
// ошибка, то будет создана паника. Во-избежание этого можно воспользоваться
// функцией SetDialer.
//
// При получении ошибки, так же, создается паника. Для переопределния этого
// поведения, нужно использовать опцию WithErrorHandler.
func Consume(ctx context.Context, topic, groupID string, handler func(context.Context, kafka.Message) error, opts ...ConsumeOpt) {
	if dialer == nil {
		if err := SetDialer(); err != nil {
			panic(err)
		}
	}
	ccfg := &consumecfg{}
	for _, opt := range opts {
		opt(ccfg)
	}
	if ccfg.errHandler == nil {
		ccfg.errHandler = panicErrHandler
	}
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Logger:  kafka.LoggerFunc(log.Default().Printf),
		Brokers: []string{os.Getenv("KAFKA_HOST")},
		GroupID: groupID,
		Topic:   topic,
		Dialer:  dialer,
	})
	defer consumer.Close()

	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				break
			}
			ccfg.errHandler(ctx, err)
			continue
		}
		if err = handler(ctx, msg); err != nil {
			if err == context.Canceled {
				break
			}
			ccfg.errHandler(ctx, err)
			continue
		}
		if err = consumer.CommitMessages(ctx, msg); err != nil {
			if err == context.Canceled {
				break
			}
			ccfg.errHandler(ctx, err)
		}
	}
}
