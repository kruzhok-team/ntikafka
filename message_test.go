package ntikafka

import (
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/trace"
)

func TestValue_Decode(t *testing.T) {
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
			err := p.Decode(jx.DecodeStr(tt.value))
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Decode() вернул ошибку: %v", err)
				}
				t.Logf("Содержимое полученной (ожидаемой) ошибки: %q", err)
				return
			}
			if tt.wantErr {
				t.Fatal("Decode() неожиданно не вернул ошибку")
			}
			if g, w := fmt.Sprintf("%+v", p), fmt.Sprintf("%+v", tt.want); g != w {
				t.Errorf("Value = %s, ожидалось %s", g, w)
			}
		})
	}
}

func BenchmarkValue_Decode(b *testing.B) {
	d := jx.Decode(nil, 0)
	p := &Value{}

	b.Run("debezium_message_value.json", func(b *testing.B) {
		input := testFile("debezium_message_value.json")
		b.ReportAllocs()
		for b.Loop() {
			d.ResetBytes(input)
			if err := p.Decode(d); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestValue_DecodeAfter(t *testing.T) {
	d := jx.Decode(nil, 512)
	s := new(testDecoder)

	tests := []struct {
		name    string
		input   string
		wantErr bool
		want    testDecoder
	}{
		{
			name:    "empty",
			input:   "",
			wantErr: false,
		},
		{
			name:    "empty after",
			input:   `{"after": null}`,
			want:    testDecoder{},
			wantErr: false,
		},
		{
			name:    "ok",
			input:   `{"after": {"int":42,"float": 3.14}}`,
			wantErr: false,
			want:    testDecoder{Int: 42, Float: 3.14},
		},
	}

	for _, tt := range tests {
		for _, schema := range []bool{false, true} {
			name := tt.name
			if schema {
				name += "/+schema"
			} else {
				name += "/-schema"
			}
			input := tt.input
			if schema && input != "" {
				input = `{"schema": {}, "payload":` + tt.input + `}`
			}

			t.Run(name, func(t *testing.T) {
				d.ResetBytes([]byte(input))
				var v Value
				err := v.DecodeAfter(d, noopSpan(), s)
				if err != nil {
					if !tt.wantErr {
						t.Errorf("DecodeAfter() failed: %v", err)
					}
					return
				}
				if tt.wantErr {
					t.Fatal("DecodeAfter() succeeded unexpectedly")
				}
				if diff := cmp.Diff(tt.want, *s); diff != "" {
					t.Errorf("testDecoder отличается от ожидаемого: %s", diff)
				}
			})
		}
	}
}

func BenchmarkValue_DecodeAfter(b *testing.B) {
	span := noopSpan()
	data := []byte(`{"after": {"int":42,"float": 3.14}}`)
	d := jx.DecodeBytes(data)
	s := new(testDecoder)
	v := new(Value)
	b.ReportAllocs()
	for b.Loop() {
		if err := v.DecodeAfter(d, span, s); err != nil {
			b.Fatal(err)
		}
	}
}

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

type testDecoder struct {
	Int   int
	Float float64
}

// Decode implements Decoder.
func (t *testDecoder) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) (err error) {
		switch string(key) {
		case "int":
			t.Int, err = d.Int()
		case "float":
			t.Float, err = d.Float64()
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}

func (t *testDecoder) SetAttributes(span trace.Span) {
	// NOTE: Метод должен успешно вызвать SetAttributes с объекта span.
	// При этом сами атрибуты мы искючаем чтобы не выполнять аллокаций
	// и не влиять ими на итоговую статистику.
	span.SetAttributes()
}
