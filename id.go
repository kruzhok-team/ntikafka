package ntikafka

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

// ID32 представляет объект из которого декодируется только поле id.
// Такой объект может декодироваться как из ключа сообщения, так и из его тела.
type ID32 struct {
	ID    int32
	Valid bool
}

func (s *ID32) SetAttributes(span trace.Span) {
	span.SetAttributes(attrID.Int(int(s.ID)))
}

func (s *ID32) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		s.Valid = true
		var err error
		switch string(key) {
		case "id":
			s.ID, err = d.Int32()
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}

// Представляет объект с ключем id, находящийся в значении сообщения.
type ID32Value struct {
	ID      ID32
	Payload Value
}

func ID32After(d *jx.Decoder, span trace.Span) (ID32Value, error) {
	var s ID32Value
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
	if err := s.ID.Decode(d); err != nil {
		return s, err
	}
	if !s.ID.Valid {
		if span != nil {
			span.AddEvent("отсутствует значение payload.after")
		}
		return s, nil
	}
	if span != nil {
		s.ID.SetAttributes(span)
	}
	return s, nil
}
