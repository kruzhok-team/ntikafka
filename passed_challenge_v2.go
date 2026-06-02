package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type PassedChallengeV2 struct {
	ID          int64
	PassedAt    time.Time
	ChallengeID int32
	UserID      int32
}

func (s *PassedChallengeV2) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrChallengeID.Int(int(s.ChallengeID)),
		attrTalentID.Int(int(s.UserID)),
		attrPassedAt.String(s.PassedAt.String()),
	)
}

func (s *PassedChallengeV2) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		var err error
		switch string(key) {
		case "id":
			s.ID, err = d.Int64()
		case "challenge_id":
			s.ChallengeID, err = d.Int32()
		case "user_id":
			s.UserID, err = d.Int32()
		case "passed_at":
			s.PassedAt, err = DecodeTimestamp(d)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
