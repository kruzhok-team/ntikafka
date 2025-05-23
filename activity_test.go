package ntikafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func BenchmarkActivityAfter(b *testing.B) {
	bench := func(name string, data []byte) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			d := jx.DecodeBytes(data)
			_, err := ActivityAfter(d, nil)
			if err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				d.ResetBytes(data)
				_, err := ActivityAfter(d, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	bench("filled", []byte(`{
		"payload": {
			"before": null,
			"after": {
				"id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527",
				"player_id": "997e0872-ec93-4b3f-b8ef-d86c8a3a7f07",
				"context_id": "2aeef629-e0e9-479b-95de-6bf4afdd671c",
				"created_at": "2025-03-03T13:22:47.212818Z"
			}
		}
	}`))

	bench("skip_empty", []byte(`{"payload": {"before": null, "after": null}}`))
}

func TestActivityAfter(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
	timeParse := func(src string) time.Time {
		v, err := time.Parse(time.RFC3339Nano, src)
		if err != nil {
			panic(err)
		}
		return v
	}
	tests := []struct {
		name    string
		value   string
		want    Activity
		wantErr bool
	}{
		{
			name:    "empty",
			value:   ``,
			want:    Activity{},
			wantErr: false,
		},
		{
			name:    "empty payload",
			value:   `{"payload": {}}`,
			want:    Activity{},
			wantErr: false,
		},
		{
			name:    "missing after",
			value:   `{"payload": {"before": {}}}`,
			want:    Activity{Payload: Payload{Valid: true, Before: jx.Raw(`{}`)}},
			wantErr: false,
		},
		{
			name:    "null after",
			value:   `{"payload": {"after": null}}`,
			want:    Activity{Payload: Payload{Valid: true}},
			wantErr: false,
		},
		{
			name:    "empty object after",
			value:   `{"payload": {"after": {}}}`,
			want:    Activity{Payload: Payload{Valid: true, After: jx.Raw(`{}`)}},
			wantErr: false,
		},
		{
			name: "valid after",
			value: oneline(`{"payload": {"after": {
				"id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527",
				"player_id": "997e0872-ec93-4b3f-b8ef-d86c8a3a7f07",
				"context_id": "2aeef629-e0e9-479b-95de-6bf4afdd671c",
				"created_at": "2025-03-03T13:22:47.212818Z"
			}}}`),
			want: Activity{
				Valid: true,
				Payload: Payload{
					Valid: true,
					After: jx.Raw(oneline(`{
						"id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527",
						"player_id": "997e0872-ec93-4b3f-b8ef-d86c8a3a7f07",
						"context_id": "2aeef629-e0e9-479b-95de-6bf4afdd671c",
						"created_at": "2025-03-03T13:22:47.212818Z"
					}`)),
				},
				ID:        uuid.MustParse("db3e8517-5bfd-4a08-ad1e-b80f4880b527"),
				PlayerID:  uuid.MustParse("997e0872-ec93-4b3f-b8ef-d86c8a3a7f07"),
				ContextID: uuid.MustParse("2aeef629-e0e9-479b-95de-6bf4afdd671c"),
				CreatedAt: timeParse("2025-03-03T13:22:47.212818Z"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		for _, span := range []trace.Span{span, nil} {
			name := tt.name
			if span != nil {
				name += "+span"
			}
			t.Run(name, func(t *testing.T) {
				got, gotErr := ActivityAfter(jx.DecodeStr(tt.value), span)
				if gotErr != nil {
					if !tt.wantErr {
						t.Errorf("ActivityAfter() вернул ошибку: %v", gotErr)
					}
					return
				}
				if tt.wantErr {
					t.Fatal("ActivityAfter() неожиданно не вернул ошибку")
				}
				if g, w := fmt.Sprintf("%+v", got), fmt.Sprintf("%+v", tt.want); g != w {
					t.Errorf("ActivityAfter() = %s, ожидалось %s", g, w)
				}
			})
		}
	}
}
