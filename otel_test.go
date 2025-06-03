package ntikafka

import (
	"context"
	"iter"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func iterspan(testname string) iter.Seq2[string, trace.Span] {
	_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
	return func(yield func(string, trace.Span) bool) {
		for _, span := range []trace.Span{span, nil} {
			name := testname
			if span == nil {
				name += "+nil"
			} else {
				name += "+span"
			}
			if !yield(name, span) {
				return
			}
		}
	}
}

// Выполнение теста дважды: со спаном и без.
func testspan(t *testing.T, testname string, test func(t *testing.T, span trace.Span)) {
	for name, span := range iterspan(testname) {
		t.Run(name, func(t *testing.T) {
			test(t, span)
		})
	}
}
