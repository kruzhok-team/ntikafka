package ntikafka

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/go-faster/jx"
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
