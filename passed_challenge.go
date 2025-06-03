package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type PassedChallenge struct {
	PassedAt    time.Time
	ChallengeID int32
	PlayerID    UUID
	ActivityID  UUID
	Valid       bool
	Payload     Value
}

func (s *PassedChallenge) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrChallengeID.Int(int(s.ChallengeID)),
		attrPlayerID.String(s.PlayerID.String()),
		attrActivityID.String(s.ActivityID.String()),
		attrPassedAt.String(s.PassedAt.String()),
	)
}

func (s *PassedChallenge) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		s.Valid = true
		var err error
		switch string(key) {
		case "challenge_id":
			s.ChallengeID, err = d.Int32()
		case "player_id":
			err = s.PlayerID.Decode(d)
		case "activity_id":
			err = s.ActivityID.Decode(d)
		case "passed_at":
			s.PassedAt, err = DecodeDate(d)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
