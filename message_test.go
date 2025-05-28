package ntikafka

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
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

func oneline(src string) string {
	lines := strings.Split(src, "\n")
	patched := make([]string, len(lines))
	for i, s := range lines {
		patched[i] = strings.TrimSpace(s)
	}
	return strings.Join(patched, " ")
}

func BenchmarkPayload_Decode(b *testing.B) {
	input := testFile("debezium_message_value.json")
	d := jx.DecodeBytes(input)
	b.ReportAllocs()
	var p Value
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
		want    Value
		wantErr bool
	}{
		{
			name:    "empty value",
			value:   "",
			wantErr: false,
		},
		{
			name:    "missing payload",
			value:   "{}",
			wantErr: false,
		},
		{
			name:    "empty payload",
			value:   `{"payload": {}}`,
			wantErr: false,
		},
		{
			name:    "null payload",
			value:   `{"payload": null}`,
			wantErr: true,
		},
		{
			name:    "null after",
			value:   `{"payload": {"after": null}}`,
			want:    Value{Valid: true},
			wantErr: false,
		},
		{
			name:    "not null after",
			value:   `{"payload": {"after": {"key": 42}}}}`,
			want:    Value{Valid: true, After: jx.Raw(`{"key": 42}`)},
			wantErr: false,
		},
		{
			name:    "null before",
			value:   `{"payload": {"before": null}}`,
			want:    Value{Valid: true},
			wantErr: false,
		},
		{
			name:    "not null before",
			value:   `{"payload": {"before": {"key": 42}}}`,
			want:    Value{Valid: true, Before: jx.Raw(`{"key": 42}`)},
			wantErr: false,
		},
		{
			name:  "from file",
			value: string(testFile("debezium_message_value.json")),
			want: Value{
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
		{
			name:    "err op",
			value:   `{"payload": {"op": 1}}`,
			wantErr: true,
		},
		{
			name:    "err ts_ms",
			value:   `{"payload": {"ts_ms": "invalid"}}`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Value
			gotErr := p.Decode(jx.DecodeStr(tt.value))
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Decode() вернул ошибку: %v", gotErr)
				}
				t.Logf("Содержимое полученной (ожидаемой) ошибки: %q", gotErr)
				return
			}
			if tt.wantErr {
				t.Fatal("Decode() неожиданно не вернул ошибку")
			}
			if g, w := fmt.Sprintf("%+v", p), fmt.Sprintf("%+v", tt.want); g != w {
				t.Errorf("Payload = %s, ожидалось %s", g, w)
			}
		})
	}
}
