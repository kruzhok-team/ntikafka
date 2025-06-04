package ntikafka

import (
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
)

var achievement = `{
	"id": 400627,
	"created_at": "2025-01-15T14:50:23.297070Z",
	"status": "+",
	"comment": "",
	"score": "AYag",
	"event_id": 3091,
	"role_id": 4,
	"document": "",
	"updated_at": "2025-05-30T13:48:55.029394Z",
	"link": "",
	"person_id": 587834,
	"team_id": null,
	"imported_at": null,
	"diploma_link": "",
	"imported_by_id": null,
	"diploma_downloaded_at": null
}`

func TestAchievement_Decode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Achievement
		wantErr bool
	}{
		{
			name:  "empty",
			input: "{}",
		},
		{
			name:  "ok",
			input: achievement,
			want: Achievement{
				ID:        400627,
				CreatedAt: parseTime("2025-01-15T14:50:23.297070Z"),
				UpdatedAt: parseTime("2025-05-30T13:48:55.029394Z"),
				Status:    "+",
				EventID:   3091,
				RoleID:    4,
				PersonID:  Null[int32]{V: 587834, Valid: true},
				TeamID:    Null[int32]{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		var s Achievement
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
			t.Errorf("Achievement отличается от ожидаемого: %s", diff)
		}
	}
}

func BenchmarkAchievement_Decode(b *testing.B) {
	data := []byte(achievement)
	d := jx.DecodeBytes(data)
	s := new(Achievement)
	b.ReportAllocs()
	for b.Loop() {
		d.ResetBytes(data)
		if err := s.Decode(d); err != nil {
			b.Fatal(err)
		}
	}
}
