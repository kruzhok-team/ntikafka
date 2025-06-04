package ntikafka

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func noopSpan() trace.Span {
	_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
	return span
}
