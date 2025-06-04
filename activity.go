package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

// Активность игрока сервиса berloga_activities.
//
// Топик: berloga_activities.public.activities
type Activity struct {
	ID        UUID
	PlayerID  UUID
	ContextID UUID
	CreatedAt time.Time
}

func (a *Activity) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrActivityID.String(a.ID.String()),
		attrPlayerID.String(a.PlayerID.String()),
		attrContextID.String(a.ContextID.String()),
		attrCreatedAt.String(a.CreatedAt.String()),
	)
}

func (a *Activity) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		var err error
		switch string(key) {
		case "id":
			err = a.ID.Decode(d)
		case "player_id":
			err = a.PlayerID.Decode(d)
		case "context_id":
			err = a.ContextID.Decode(d)
		case "created_at":
			a.CreatedAt, err = DecodeDate(d)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
