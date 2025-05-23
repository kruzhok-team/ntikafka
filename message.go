package ntikafka

import (
	"time"
	"unsafe"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
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
