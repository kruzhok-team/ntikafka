package ntikafka

import (
	"testing"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func TestDecodeDate(t *testing.T) {
	got, err := DecodeDate(jx.DecodeBytes([]byte(`6822`)))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(1988, time.September, 5, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("DecodeDate: %v, вместо %v", got, want)
	}
}

func parseUUID(s string) UUID {
	return (UUID)(uuid.MustParse(s))
}

func parseTime(s string) time.Time {
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return v
}

// Бенчмарк статичного Nullable для сравнения с дженериком.
func BenchmarkNullInt32_Decode(b *testing.B) {
	for _, data := range []string{`null`, `42`} {
		b.Run(data, func(b *testing.B) {
			src := []byte(data)
			d := jx.DecodeBytes(src)
			var v NullInt32
			b.ReportAllocs()
			for b.Loop() {
				d.ResetBytes(src)
				if err := v.Decode(d); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNull_Decode(b *testing.B) {
	for _, data := range []string{`null`, `42`} {
		b.Run("value/int32/"+data, func(b *testing.B) {
			src := []byte(data)
			d := jx.DecodeBytes(src)
			b.ReportAllocs()
			for b.Loop() {
				d.ResetBytes(src)
				var v Null[int32]
				if err := v.DecodeValue(d, DecodeInt32); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, data := range []string{`null`, `"a9af45a7-47fd-42ee-9983-1e6c9284d386"`} {
		b.Run("receiver/[16]byte/"+data[:4], func(b *testing.B) {
			src := []byte(data)
			d := jx.DecodeBytes(src)
			b.ReportAllocs()
			for b.Loop() {
				d.ResetBytes(src)
				var v Null[[16]byte]
				if err := v.DecodeReceiver(d, func(d *jx.Decoder) error {
					return DecodeUUID(d, &v.V)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	for _, data := range []string{`null`, `"a9af45a7-47fd-42ee-9983-1e6c9284d386"`} {
		b.Run("receiver/UUID/"+data[:4], func(b *testing.B) {
			src := []byte(data)
			d := jx.DecodeBytes(src)
			b.ReportAllocs()
			for b.Loop() {
				d.ResetBytes(src)
				var v Null[UUID]
				if err := v.DecodeReceiver(d, v.V.Decode); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestNull_Decoder(t *testing.T) {
	for src, want := range map[string]Null[UUID]{
		`null`: Null[UUID]{
			V:     UUID{},
			Valid: false,
		},
		`"a9af45a7-47fd-42ee-9983-1e6c9284d386"`: Null[UUID]{
			V:     UUID(uuid.MustParse("a9af45a7-47fd-42ee-9983-1e6c9284d386")),
			Valid: true,
		},
	} {
		t.Run(src, func(t *testing.T) {
			var v Null[UUID]
			if err := v.Decode(jx.DecodeBytes([]byte(src))); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(want, v); diff != "" {
				t.Errorf("Null[UUID].Decode(%s):\n%s", src, diff)
			}
		})
	}
}

func TestPoint(t *testing.T) {
	d := jx.Decode(nil, 512)
	for _, tt := range []struct {
		name  string
		input string
		want  Point
	}{
		{
			name:  "null",
			input: `null`,
		},
		{
			name: "ok",
			input: `{
				"x": 82.993774,
				"y": 55.065243,
				"wkb": "AQEAAABeZ0P+mb9UQH+l8+FZiEtA",
				"srid": null
			}`,
			want: Point{
				X:     82.993774,
				Y:     55.065243,
				Valid: true,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var p Point
			d.ResetBytes([]byte(tt.input))
			if err := p.Decode(d); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, p); diff != "" {
				t.Errorf("Результат Decode() содержит отличия: %s", diff)
			}
		})
	}
}
