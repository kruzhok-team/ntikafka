package ntikafka

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var attrID = attribute.Key("id")

type ID32 struct {
	ID      int32
	Valid   bool
	Payload Payload
}

func (a *ID32) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		a.Valid = true
		var err error
		switch string(key) {
		case "id":
			a.ID, err = d.Int32()
		default:
			return d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}

func ID32After(d *jx.Decoder, span trace.Span) (ID32, error) {
	var s ID32
	if err := s.Payload.Decode(d); err != nil {
		return s, err
	}
	if s.Payload.After == nil {
		if span != nil {
			span.AddEvent("отсутствует значение payload.after")
		}
		return s, nil
	}
	d.ResetBytes(s.Payload.After)
	if err := s.Decode(d); err != nil {
		return s, err
	}
	if !s.Valid {
		if span != nil {
			span.AddEvent("отсутствует значение payload.after")
		}
		return s, nil
	}
	if span != nil {
		span.SetAttributes(attrID.Int(int(s.ID)))
	}
	return s, nil
}
