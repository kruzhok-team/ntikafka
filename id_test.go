package ntikafka

import (
	"context"
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestID32After(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
	tests := []struct {
		name    string
		value   string
		want    ID32Value
		wantErr bool
	}{
		{
			name:    "empty",
			value:   ``,
			wantErr: false,
		},
		{
			name:    "empty payload",
			value:   `{"payload": {}}`,
			wantErr: false,
		},
		{
			name:  "valid payload",
			value: `{"payload": {"after": {"id": 42}}}`,
			want: ID32Value{
				ID:      ID32{ID: 42, Valid: true},
				Payload: Value{Valid: true, After: jx.Raw(`{"id": 42}`)},
			},
		},
		{
			name:  "valid schemaless",
			value: `{"after": {"id": 42}}`,
			want: ID32Value{
				ID:      ID32{ID: 42, Valid: true},
				Payload: Value{Valid: true, After: jx.Raw(`{"id": 42}`)},
			},
		},
	}
	for _, tt := range tests {
		for _, span := range []trace.Span{span, nil} {
			name := tt.name
			if span != nil {
				name += "+span"
			}
			t.Run(name, func(t *testing.T) {
				got, gotErr := ID32After(jx.DecodeStr(tt.value), span)
				if gotErr != nil {
					if !tt.wantErr {
						t.Errorf("ID32After() вернул ошибку: %v", gotErr)
					}
					return
				}
				if tt.wantErr {
					t.Fatal("ID32After() неожиданно не вернул ошибку")
				}
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ID32After() = %s", diff)
				}
			})
		}
	}
}
