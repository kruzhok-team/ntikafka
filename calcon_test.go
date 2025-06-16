package ntikafka

import (
	"fmt"
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
)

const userscore = `{
	"id": 58929,
	"user_id": 125844,
	"competence_id": 4,
	"year": 2022,
	"score": 3.14,
	"details": {},
	"created_at":"2023-11-21T17:46:45.062924Z",
	"updated_at":"2024-11-21T17:46:45.062924Z"
}`

func TestUserScore_Decode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    UserScore
		wantErr bool
	}{
		{
			name:  "empty",
			input: "{}",
		},
		{
			name:  "ok",
			input: userscore,
			want: UserScore{
				ID:           58929,
				UserID:       125844,
				CompetenceID: 4,
				Year:         2022,
				Score:        3.14,
				CreatedAt:    parseTime("2023-11-21T17:46:45.062924Z"),
				UpdatedAt:    parseTime("2024-11-21T17:46:45.062924Z"),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		var s UserScore
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
			t.Errorf("UserScore отличается от ожидаемого: %s", diff)
		}
	}
}

func BenchmarkUserScore_Decode(b *testing.B) {
	data := []byte(userscore)
	d := jx.DecodeBytes(data)
	s := new(UserScore)
	for _, details := range []bool{false, true} {
		b.Run(fmt.Sprintf("details=%v", details), func(b *testing.B) {
			s.WithDetails = details
			b.ReportAllocs()
			for b.Loop() {
				d.ResetBytes(data)
				if err := s.Decode(d); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
