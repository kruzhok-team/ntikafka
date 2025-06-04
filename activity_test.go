package ntikafka

import (
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
)

const activity = `{
	"id":"49880c1e-6400-4d11-bb06-854721e8a56c",
	"created_at":"2023-11-21T17:46:45.062924Z",
	"context_id":"9b9a23ed-c637-439b-84cd-b10bec5855c7",
	"player_id":"6c66bd93-8c8f-4cc1-b27f-e78ab44681e5",
	"scores":null,
	"quarantine":null,
	"artefact_id":null,
	"app_version":"test.0.0"
}`

func TestActivity_Decode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Activity
		wantErr bool
	}{
		{
			name:  "empty",
			input: "{}",
		},
		{
			name:  "ok",
			input: activity,
			want: Activity{
				ID:        parseUUID("49880c1e-6400-4d11-bb06-854721e8a56c"),
				PlayerID:  parseUUID("6c66bd93-8c8f-4cc1-b27f-e78ab44681e5"),
				ContextID: parseUUID("9b9a23ed-c637-439b-84cd-b10bec5855c7"),
				CreatedAt: parseTime("2023-11-21T17:46:45.062924Z"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		var s Activity
		if err := s.Decode(jx.DecodeStr(tt.input)); err != nil {
			if !tt.wantErr {
				t.Errorf("Decode() вернул ошибку: %v", err)
			}
			return
		}
		if tt.wantErr {
			t.Fatal("Decode() неожиданно не вернул ошибку")
		}
		if diff := cmp.Diff(tt.want, s); diff != "" {
			t.Errorf("Activity отличается от ожидаемого: %s", diff)
		}
	}
}

func BenchmarkActivity_Decode(b *testing.B) {
	data := []byte(activity)
	d := jx.DecodeBytes(data)
	s := new(Activity)
	b.ReportAllocs()
	for b.Loop() {
		d.ResetBytes(data)
		if err := s.Decode(d); err != nil {
			b.Fatal(err)
		}
	}
}
