package ntikafka

import (
	"time"
	"unsafe"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
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

type Date time.Time

func (s *Date) Decode(d *jx.Decoder) error {
	t, err := DecodeDate(d)
	if err != nil {
		return err
	}
	*s = Date(t)
	return nil
}

// DecodeDate декодирует строку даты/времени в формате [time.RFC3339Nano]
// Функция использует unsafe чтобы избежать аллокации.
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

// Value представляет тело сообщения, записанное коннектором Debezium.
//
// При наличии ключей schema или payload,
// объект трактуется как значение со схемой,
// а свойства структры заполнятся из ключа payload.
//
// Before и After не nil, если имеются соответствующие ключи,
// а также, если значение под этим ключем не null.
type Value struct {
	Valid     bool // Наличие какого-либо ключа в объекте (payload).
	Timestamp int64
	Operation DebeziumOperation
	Before    jx.Raw
	After     jx.Raw
}

func (p *Value) Decode(d *jx.Decoder) error {
	return p.decode(d, p.decodePayloadKey)
}

func (p *Value) decode(d *jx.Decoder, keyDecoder func(d *jx.Decoder, key []byte) error) error {
	if t := d.Next(); t == jx.Null || t == jx.Invalid {
		return nil
	}
	return d.ObjBytes(func(d *jx.Decoder, key []byte) (err error) {
		switch string(key) {
		case "schema":
			err = d.Skip()
		case "payload":
			err = d.ObjBytes(keyDecoder)
		default:
			err = keyDecoder(d, key)
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}

func (p *Value) decodePayloadKey(d *jx.Decoder, key []byte) (err error) {
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
}

type Decoder interface {
	Decode(*jx.Decoder) error
}

type SpanAttrDecoder interface {
	Decoder
	SetAttributes(span trace.Span)
}

// Декодирование только значения after в объект s, если это значение имеется.
func (p *Value) DecodeAfter(d *jx.Decoder, span trace.Span, s SpanAttrDecoder) (err error) {
	err = p.decode(d, p.decodePayloadOnlyAfter)
	if err != nil {
		return err
	}
	if p.After == nil {
		span.AddEvent("отсутствует значение after")
		return nil
	}
	d.ResetBytes(p.After)
	if err = s.Decode(d); err != nil {
		return errors.Wrap(err, "after")
	}
	s.SetAttributes(span)
	return nil
}

func (p *Value) decodePayloadOnlyAfter(d *jx.Decoder, key []byte) (err error) {
	p.Valid = true
	switch string(key) {
	case "after":
		if d.Next() == jx.Null {
			err = d.Skip()
			break
		}
		p.After, err = d.Raw()
	default:
		err = d.Skip()
	}
	if err != nil {
		err = errors.Wrap(err, string(key))
	}
	return err
}
