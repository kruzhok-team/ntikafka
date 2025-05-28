package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	attrActivityID = attribute.Key("activity_id")
	attrPlayerID   = attribute.Key("player_id")
	attrContextID  = attribute.Key("context_id")
)

// Активность игрока сервиса berloga_activities.
//
// Топик: berloga_activities.public.activities
type Activity struct {
	Payload   Value
	Valid     bool
	ID        [16]byte
	PlayerID  [16]byte
	ContextID [16]byte
	CreatedAt time.Time
}

func (a *Activity) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		a.Valid = true
		var err error
		switch string(key) {
		case "id":
			err = DecodeUUID(d, &a.ID)
		case "player_id":
			err = DecodeUUID(d, &a.PlayerID)
		case "context_id":
			err = DecodeUUID(d, &a.ContextID)
		case "created_at":
			a.CreatedAt, err = DecodeDate(d)
		default:
			return d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}

func ActivityAfter(d *jx.Decoder, span trace.Span) (Activity, error) {
	var act Activity
	if err := act.Payload.Decode(d); err != nil {
		return act, err
	}
	if act.Payload.After == nil {
		if span != nil {
			span.AddEvent("отсутствует значение payload.after")
		}
		return act, nil
	}
	d.ResetBytes(act.Payload.After)
	if err := act.Decode(d); err != nil {
		return act, err
	}
	if !act.Valid {
		if span != nil {
			span.AddEvent("отсутствует значение payload.after")
		}
		return act, nil
	}
	if span != nil {
		span.SetAttributes(
			attrActivityID.String(uuid.UUID(act.ID).String()),
			attrPlayerID.String(uuid.UUID(act.PlayerID).String()),
			attrContextID.String(uuid.UUID(act.ContextID).String()),
		)
	}
	return act, nil
}
