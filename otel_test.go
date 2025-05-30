package ntikafka

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Выполнение теста дважды: со спаном и без.
func testspan(t *testing.T, testname string, test func(t *testing.T, span trace.Span)) {
	_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
	for _, span := range []trace.Span{span, nil} {
		name := testname
		if span == nil {
			name += "+nil"
		} else {
			name += "+span"
		}
		t.Run(name, func(t *testing.T) {
			test(t, span)
		})
	}
}
