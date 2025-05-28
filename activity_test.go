package ntikafka

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
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

	bench("filled_payload", []byte(`{
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

	bench("filled_schemaless", []byte(`{
		"before": null,
		"after": {
			"id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527",
			"player_id": "997e0872-ec93-4b3f-b8ef-d86c8a3a7f07",
			"context_id": "2aeef629-e0e9-479b-95de-6bf4afdd671c",
			"created_at": "2025-03-03T13:22:47.212818Z"
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
			want:    Activity{Payload: Value{Valid: true, Before: jx.Raw(`{}`)}},
			wantErr: false,
		},
		{
			name:    "null after",
			value:   `{"payload": {"after": null}}`,
			want:    Activity{Payload: Value{Valid: true}},
			wantErr: false,
		},
		{
			name:    "empty object after",
			value:   `{"payload": {"after": {}}}`,
			want:    Activity{Payload: Value{Valid: true, After: jx.Raw(`{}`)}},
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
				Payload: Value{
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
		{
			name: "without schema",
			value: oneline(`{
				"before":null,
				"after":{
					"id":"49880c1e-6400-4d11-bb06-854721e8a56c",
					"created_at":"2023-11-21T17:46:45.062924Z",
					"context_id":"9b9a23ed-c637-439b-84cd-b10bec5855c7",
					"player_id":"6c66bd93-8c8f-4cc1-b27f-e78ab44681e5",
					"scores":null,
					"quarantine":null,
					"artefact_id":null,
					"app_version":"test.0.0"
				},
				"op":"r",
				"ts_ms":1748417064848,
				"ts_us":1748417064848402,
				"ts_ns":1748417064848402160
			}`),
			want: Activity{
				Valid: true,
				Payload: Value{
					Valid:     true,
					Timestamp: 1748417064848,
					Operation: DebeziumOperationRead,
					After: jx.Raw(oneline(`{
						"id":"49880c1e-6400-4d11-bb06-854721e8a56c",
						"created_at":"2023-11-21T17:46:45.062924Z",
						"context_id":"9b9a23ed-c637-439b-84cd-b10bec5855c7",
						"player_id":"6c66bd93-8c8f-4cc1-b27f-e78ab44681e5",
						"scores":null,
						"quarantine":null,
						"artefact_id":null,
						"app_version":"test.0.0"
					}`)),
				},
				ID:        uuid.MustParse("49880c1e-6400-4d11-bb06-854721e8a56c"),
				PlayerID:  uuid.MustParse("6c66bd93-8c8f-4cc1-b27f-e78ab44681e5"),
				ContextID: uuid.MustParse("9b9a23ed-c637-439b-84cd-b10bec5855c7"),
				CreatedAt: timeParse("2023-11-21T17:46:45.062924Z"),
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
				if diff := cmp.Diff(tt.want, got); diff != "" {
					t.Errorf("ActivityAfter() = %s", diff)
				}
			})
		}
	}
}
