package ntikafka

import (
	"fmt"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
)

// Null для любого типа.
type Null[T any] struct {
	V     T
	Valid bool
}

// Decode выполняет декодирование только если T реализует [Decoder]!
func (n *Null[T]) Decode(d *jx.Decoder) error {
	if d.Next() == jx.Null {
		return d.Skip()
	}
	if v, ok := any(&n.V).(Decoder); ok {
		(*n).Valid = true
		return v.Decode(d)
	}
	panic(fmt.Sprintf("%T does not implement Decoder", n.V))
}

// DecodeReceiver от next ожидает, что функция сама заполнит значение свойства V.
func (n *Null[T]) DecodeReceiver(d *jx.Decoder, next func(d *jx.Decoder) error) error {
	if d.Next() == jx.Null {
		return d.Skip()
	}
	(*n).Valid = true
	return next(d)
}

// DecodeValue копирует возвращаемое значение из next, если в декодере не null.
func (n *Null[T]) DecodeValue(d *jx.Decoder, next func(d *jx.Decoder) (T, error)) error {
	if d.Next() == jx.Null {
		return d.Skip()
	}
	v, err := next(d)
	if err != nil {
		return err
	}
	(*n).Valid = true
	(*n).V = v
	return nil
}

// UUID для использования в Null[UUID].DecodeReceiver.
type UUID [16]byte

func (u *UUID) Decode(d *jx.Decoder) error {
	return DecodeUUID(d, (*[16]byte)(u))
}

func (u UUID) String() string {
	return uuid.UUID(u).String()
}

// DecodeInt32 для использования в Null[int32].DecodeValue.
func DecodeInt32(d *jx.Decoder) (int32, error) {
	return d.Int32()
}

// DecodeFloat64 для использования в Null[float64].DecodeValue.
func DecodeFloat64(d *jx.Decoder) (float64, error) {
	return d.Float64()
}

type NullInt32 struct {
	V     int32
	Valid bool
}

func (n *NullInt32) Decode(d *jx.Decoder) error {
	if d.Next() == jx.Null {
		return d.Skip()
	}
	v, err := d.Int32()
	if err != nil {
		return err
	}
	(*n).Valid = true
	(*n).V = v
	return nil
}

func deNullStr(d *jx.Decoder) (string, error) {
	if d.Next() == jx.Null {
		return "", d.Skip()
	}
	return d.Str()
}

// Точка из PostgreSQL сформированная Debezium.
type Point struct {
	X     float64
	Y     float64
	Valid bool
}

func (p *Point) Decode(d *jx.Decoder) error {
	if d.Next() == jx.Null {
		return d.Skip()
	}
	return d.ObjBytes(func(d *jx.Decoder, key []byte) (err error) {
		p.Valid = true
		switch string(key) {
		case "x":
			p.X, err = d.Float64()
		case "y":
			p.Y, err = d.Float64()
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
