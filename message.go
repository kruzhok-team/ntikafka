package ntikafka

import (
	"time"
	"unsafe"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func DecodeUUID(d *jx.Decoder, dst *[16]byte) error {
	raw, err := d.StrBytes()
	if err != nil {
		return err
	}
	id, err := uuid.ParseBytes(raw)
	if err != nil {
		return err
	}
	for i, s := range id {
		dst[i] = s
	}
	return nil
}

func DecodeDate(d *jx.Decoder) (time.Time, error) {
	var t time.Time
	raw, err := d.StrBytes()
	if err != nil {
		return t, err
	}
	return time.Parse(time.RFC3339Nano, *(*string)(unsafe.Pointer(&raw)))
}

type DebeziumOperation string

const (
	DebeziumOperationCreate   DebeziumOperation = "c"
	DebeziumOperationUpdate   DebeziumOperation = "u"
	DebeziumOperationDelete   DebeziumOperation = "d"
	DebeziumOperationRead     DebeziumOperation = "r"
	DebeziumOperationTruncate DebeziumOperation = "t"
	DebeziumOperationMessage  DebeziumOperation = "m"
)

// Payload представляет данные в теле сообщения под ключом payload.
//
// Before и After не nil, если в payload имеется одноименный ключ,
// а также, если значение под этим ключем не null.
type Payload struct {
	Valid     bool
	Timestamp int64
	Operation DebeziumOperation
	Before    jx.Raw
	After     jx.Raw
}

func (p *Payload) Decode(d *jx.Decoder) error {
	if t := d.Next(); t == jx.Null || t == jx.Invalid {
		return nil
	}
	var err error
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		if string(key) != "payload" {
			return d.Skip()
		}
		err = d.ObjBytes(func(d *jx.Decoder, key []byte) error {
			p.Valid = true
			switch string(key) {
			case "before":
				if d.Next() == jx.Null {
					err = d.Skip()
					break
				}
				p.Before, err = d.Raw()
			case "after":
				if d.Next() == jx.Null {
					err = d.Skip()
					break
				}
				p.After, err = d.Raw()
			case "op":
				v, err := d.Str()
				if err != nil {
					return errors.Wrap(err, "op")
				}
				p.Operation = DebeziumOperation(v)
			case "ts_ms":
				p.Timestamp, err = d.Int64()
			default:
				err = d.Skip()
			}
			if err != nil {
				err = errors.Wrap(err, string(key))
			}
			return err
		})
		if err != nil {
			err = errors.Wrap(err, "payload")
		}
		return err
	})
}

var (
	attrActivityID = attribute.Key("activity_id")
	attrPlayerID   = attribute.Key("player_id")
	attrContextID  = attribute.Key("context_id")
)

// Activity представляет топик: berloga_activities.public.activities
type Activity struct {
	Payload   Payload
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
