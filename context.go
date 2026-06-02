package ntikafka

import (
	"context"
	"fmt"
	"os"
)

type contextKey int

const clientIDKey contextKey = iota

// Установка идентификатора клиента.
func WithClientID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, clientIDKey, id)
}

// Установка идентификатора консьюмера на основе переменной KAFKA_GROUP и номера воркера.
func WithConsumerClientID(ctx context.Context, worker int) context.Context {
	if v := os.Getenv(EnvGroupID); v != "" {
		ctx = context.WithValue(ctx, clientIDKey, fmt.Sprintf("%s-%v", v, worker))
	}
	return ctx
}

func clientID(ctx context.Context) string {
	if v := ctx.Value(clientID); v != nil {
		return v.(string)
	}
	return os.Getenv(EnvClientID)
}
