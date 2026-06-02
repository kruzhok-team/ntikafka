package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type UserScore struct {
	ID           int32
	UserID       int32
	CompetenceID int32
	Year         int32
	Score        float64
	CreatedAt    time.Time
	UpdatedAt    time.Time

	WithDetails bool // If true, Decode links Details to `details`.
	Details     []byte
}

func (s *UserScore) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrTalentID.Int(int(s.UserID)),
		attrCompetenceID.Int(int(s.CompetenceID)),
		attrYear.Int(int(s.Year)),
		attrScore.Float64(s.Score),
		attrCreatedAt.String(s.CreatedAt.String()),
		attrUpdatedAt.String(s.UpdatedAt.String()),
	)
}

func (s *UserScore) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		var err error
		switch string(key) {
		case "id":
			s.ID, err = d.Int32()
		case "user_id":
			s.UserID, err = d.Int32()
		case "competence_id":
			s.CompetenceID, err = d.Int32()
		case "year":
			s.Year, err = d.Int32()
		case "score":
			s.Score, err = d.Float64()
		case "created_at":
			s.CreatedAt, err = DecodeTimestamp(d)
		case "updated_at":
			s.UpdatedAt, err = DecodeTimestamp(d)
		case "details":
			if s.WithDetails {
				s.Details, err = d.Raw()
			} else {
				err = d.Skip()
			}
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
