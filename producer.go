package ntikafka

import (
	_ "embed"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// Producer создает kafka.Writer для записи в топик яндекс.клауда.
// Сообщения в топик пишутся асинхронно; при завершении работы процесса нужно
// вызвать Close() для отправки запланированных сообщений.
func Producer(clientID, user, topic string) (*kafka.Writer, error) {
	password := os.Getenv("KAFKA_" + strings.ToUpper(user) + "_PASSWORD")
	mechanism, err := scram.Mechanism(scram.SHA512, name(user), password)
	if err != nil {
		return nil, fmt.Errorf("scram.Mechanism: %w", err)
	}
	transport := &kafka.Transport{
		TLS:      tlscfg,
		SASL:     mechanism,
		ClientID: clientID,
	}
	return &kafka.Writer{
		Logger:       kafka.LoggerFunc(log.Default().Printf),
		Addr:         kafka.TCP(os.Getenv("KAFKA_HOST")),
		Topic:        name(topic),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Transport:    transport,
	}, nil
}
