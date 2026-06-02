package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type User struct {
	ID             int32
	Active         bool
	Email          string
	Phone          string
	PhoneConfirmed bool
	LastName       string
	FirstName      string
	MiddleName     string
	NoMiddleName   bool
	DateJoined     time.Time
	Sex            Null[string]
	Birthday       Null[time.Time]
	LastLogin      Null[time.Time]
}

func (u *User) SetAttributes(span trace.Span) {
	span.SetAttributes(attrTalentID.Int(int(u.ID)))
	if u.Email != "" {
		span.SetAttributes(attrUserEmail.String(u.Email))
	}
	if u.Phone != "" {
		span.SetAttributes(attrUserPhone.String(u.Phone))
	}
}

func (u *User) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, k []byte) (err error) {
		switch string(k) {
		case "id":
			u.ID, err = d.Int32()
		case "is_active":
			u.Active, err = d.Bool()
		case "email":
			u.Email, err = d.Str()
		case "phone":
			u.Phone, err = d.Str()
		case "phone_confirmed":
			u.PhoneConfirmed, err = d.Bool()
		case "last_name":
			u.LastName, err = d.Str()
		case "first_name":
			u.FirstName, err = d.Str()
		case "middle_name":
			u.MiddleName, err = d.Str()
		case "no_middle_name":
			u.NoMiddleName, err = d.Bool()
		case "date_joined":
			u.DateJoined, err = DecodeTimestamp(d)
		case "sex":
			err = u.Sex.DecodeValue(d, DecodeString)
		case "birthday":
			err = u.Birthday.DecodeValue(d, DecodeDate)
		case "last_login":
			err = u.LastLogin.DecodeValue(d, DecodeTimestamp)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(k))
		}
		return nil
	})
}
