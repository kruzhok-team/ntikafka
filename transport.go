package ntikafka

import (
	"fmt"
	"os"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type connectCfg struct {
	host  string
	user  string
	pass  string
	topic string
}

func newConnectCfg() (connectCfg, error) {
	cfg := connectCfg{}
	for key, prop := range map[string]*string{
		"KAFKA_HOST":  &cfg.host,
		"KAFKA_USER":  &cfg.user,
		"KAFKA_PASS":  &cfg.pass,
		"KAFKA_TOPIC": &cfg.topic,
	} {
		v := os.Getenv(key)
		if v == "" {
			return cfg, fmt.Errorf("отсутствует значение переменной окружения %s", key)
		}
		*prop = v
	}
	return cfg, nil
}

func (c connectCfg) mechanism() (sasl.Mechanism, error) {
	if c.user == "" {
		return nil, nil
	}
	return scram.Mechanism(scram.SHA512, c.user, c.pass)
}
