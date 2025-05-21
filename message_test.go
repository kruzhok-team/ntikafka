package ntikafka

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace/noop"
)

func testFile(fname string) []byte {
	f, err := os.Open(path.Join("testdata", fname))
	if err != nil {
		panic(err)
	}
	input, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	return input
}

func BenchmarkPayload_Decode(b *testing.B) {
	input := testFile("debezium_message_value.json")
	d := jx.DecodeBytes(input)
	b.ReportAllocs()
	var p Payload
	for b.Loop() {
		d.ResetBytes(input)
		if err := p.Decode(d); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPayload_Decode(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    Payload
		wantErr bool
	}{
		{
			name:    "empty value",
			value:   "",
			wantErr: false,
		},
		{
			name:    "empty after",
			value:   `{"after": null}`,
			wantErr: false,
		},
		{
			name:  "from file",
			value: string(testFile("debezium_message_value.json")),
			want: Payload{
				Valid:     true,
				Timestamp: 1741008167284,
				Operation: DebeziumOperationUpdate,
				Before:    nil,
				After: jx.Raw(`{
      "id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527",
      "created_at": "2025-03-03T13:22:47.212818Z",
      "score": null
    }`),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Payload
			gotErr := p.Decode(jx.DecodeStr(tt.value))
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Decode() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Decode() succeeded unexpectedly")
			}
			if g, w := fmt.Sprintf("%+v", p), fmt.Sprintf("%+v", tt.want); g != w {
				t.Errorf("Payload = %s, want %s", g, w)
			}
		})
	}
}

func BenchmarkActivityAfter(b *testing.B) {
	_, span := noop.NewTracerProvider().Tracer("").Start(
		context.Background(), "",
	)

	bench := func(name string, data []byte) {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			d := jx.DecodeBytes(data)
			_, err := ActivityAfter(d, span)
			if err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				d.ResetBytes(data)
				_, err := ActivityAfter(d, span)
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
				"created_at": "2025-03-03T13:22:47.212818Z",
				"score": null
			}
		}
	}`))

	bench("skip_empty", []byte(`{"payload": {"before": null, "after": null}}`))
}

func TestActivityAfter(t *testing.T) {
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
			name:  "valid after",
			value: `{"payload": {"after": {"id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527"}}}`,
			want: Activity{
				Valid: true,
				Payload: Payload{
					Valid: true,
					After: jx.Raw(`{"id": "db3e8517-5bfd-4a08-ad1e-b80f4880b527"}`),
				},
				ID: uuid.MustParse("db3e8517-5bfd-4a08-ad1e-b80f4880b527"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, span := noop.NewTracerProvider().Tracer("").Start(context.Background(), "")
			got, gotErr := ActivityAfter(jx.DecodeStr(tt.value), span)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("ActivityAfter() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("ActivityAfter() succeeded unexpectedly")
			}
			if g, w := fmt.Sprintf("%+v", got), fmt.Sprintf("%+v", tt.want); g != w {
				t.Errorf("ActivityAfter() = %s, want %s", g, w)
			}
		})
	}
}
