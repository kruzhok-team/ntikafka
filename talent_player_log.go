package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type TalentPlayerLog struct {
	ID           int32
	CreatedAt    time.Time
	TalentUserID int32
	Action       TalentPlayerLogAction
	PlayerID     UUID
}

type TalentPlayerLogAction string

const (
	TalentPlayerLogActionConnect    TalentPlayerLogAction = "connect"
	TalentPlayerLogActionDisconnect TalentPlayerLogAction = "disconnect"
)

func (a *TalentPlayerLogAction) Decode(d *jx.Decoder) error {
	v, err := d.Str()
	if err != nil {
		return err
	}
	*a = TalentPlayerLogAction(v)
	return nil
}

func (s *TalentPlayerLog) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrTalentPlayerLogID.Int(int(s.ID)),
		attrCreatedAt.String(s.CreatedAt.String()),
		attrTalentID.Int(int(s.TalentUserID)),
		attrPlayerID.String(s.PlayerID.String()),
		attrTalentPlayerLogAction.String(string(s.Action)),
	)
}

func (s *TalentPlayerLog) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		var err error
		switch string(key) {
		case "id":
			s.ID, err = d.Int32()
		case "created_at":
			s.CreatedAt, err = DecodeTimestamp(d)
		case "talent_user_id":
			s.TalentUserID, err = d.Int32()
		case "player_id":
			err = s.PlayerID.Decode(d)
		case "action":
			err = s.Action.Decode(d)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
