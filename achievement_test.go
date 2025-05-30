package ntikafka

import (
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/trace"
)

var achievementValue = `{
	"before": null,
	"after": {
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
	},
	"source": {
		"version": "3.0.8.Final",
		"connector": "postgresql",
		"name": "talent",
		"ts_ms": 1748612935315,
		"snapshot": "false",
		"db": "talent",
		"sequence": "[\"219084173424\",\"219084181840\"]",
		"ts_us": 1748612935315271,
		"ts_ns": 1748612935315271000,
		"schema": "public",
		"table": "package_achievement",
		"txId": 1263401767,
		"lsn": 219084181840,
		"xmin": null
	},
	"transaction": null,
	"op": "u",
	"ts_ms": 1748612935387,
	"ts_us": 1748612935387042,
	"ts_ns": 1748612935387042881
}`

func TestAchievementAfter(t *testing.T) {
	tests := []struct {
		name    string // description of this test case
		value   string
		want    Achievement
		wantErr bool
	}{
		{
			name:  "from-stage",
			value: achievementValue,
			want: Achievement{
				ID:        400627,
				CreatedAt: parseTime("2025-01-15T14:50:23.297070Z"),
				UpdatedAt: parseTime("2025-05-30T13:48:55.029394Z"),
				Status:    "+",
				EventID:   3091,
				RoleID:    4,
				PersonID:  Null[int32]{V: 587834, Valid: true},
				TeamID:    Null[int32]{},
				Valid:     true,
				Payload: Value{
					Valid:     true,
					Timestamp: 1748612935387,
					Operation: DebeziumOperationUpdate,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		testspan(t, tt.name, func(t *testing.T, span trace.Span) {
			got, gotErr := AchievementAfter(jx.DecodeStr(tt.value), span)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("AchievementAfter() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("AchievementAfter() неожиданно не вернул ошибку")
			}
			got.Payload.Before = nil
			got.Payload.After = nil
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("AchievementAfter() = %s", diff)
			}
		})
	}
}

func BenchmarkAchievementAfter(b *testing.B) {
	bench := func(name string, data []byte) {
		b.Run(name, func(b *testing.B) {
			d := jx.DecodeBytes(data)
			b.ReportAllocs()
			for b.Loop() {
				d.ResetBytes(data)
				if _, err := AchievementAfter(d, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	bench("schemaless_stage", []byte(achievementValue))

	bench("schemaless_minimal", []byte(`{
		"before": null,
		"after": {
			"id": 400627,
			"created_at": "2025-01-15T14:50:23.297070Z",
			"status": "+",
			"event_id": 3091,
			"role_id": 4,
			"updated_at": "2025-05-30T13:48:55.029394Z",
			"person_id": 587834,
			"team_id": null
		}
	}`))
}
