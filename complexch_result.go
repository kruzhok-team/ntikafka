package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type ComplexchResult struct {
	ID          int64
	UserID      int32
	ChallengeID int32
	Passed      bool
	PassedWas   bool
	Score       Null[float64]
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Valid       bool
	Payload     Value
}

func (s *ComplexchResult) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrComplexchResultID.Int64(s.ID),
		attrTalentID.Int(int(s.UserID)),
		attrChallengeID.Int(int(s.ChallengeID)),
		attrComplexchResultPassed.Bool(s.Passed),
		attrComplexchResultPassedWas.Bool(s.PassedWas),
		attrCreatedAt.String(s.CreatedAt.String()),
		attrUpdatedAt.String(s.UpdatedAt.String()),
	)
	if s.Score.Valid {
		span.SetAttributes(attrComplexchResultScore.Float64(s.Score.V))
	}
}

func (s *ComplexchResult) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		s.Valid = true
		var err error
		switch string(key) {
		case "id":
			s.ID, err = d.Int64()
		case "user_id":
			s.UserID, err = d.Int32()
		case "ch_id":
			s.ChallengeID, err = d.Int32()
		case "passed":
			s.Passed, err = d.Bool()
		case "passed_was":
			s.PassedWas, err = d.Bool()
		case "score":
			err = s.Score.DecodeValue(d, DecodeFloat64)
		case "created_at":
			s.CreatedAt, err = DecodeDate(d)
		case "updated_at":
			s.UpdatedAt, err = DecodeDate(d)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
