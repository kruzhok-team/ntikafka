package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type Achievement struct {
	ID        int32
	CreatedAt time.Time
	UpdatedAt time.Time
	Status    string
	EventID   int32
	RoleID    int32
	PersonID  Null[int32]
	TeamID    Null[int32]
}

func (a *Achievement) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrAchievementID.Int(int(a.ID)),
		attrCreatedAt.String(a.CreatedAt.String()),
		attrUpdatedAt.String(a.UpdatedAt.String()),
		attrAchievementStatus.String(a.Status),
		attrEventID.Int(int(a.EventID)),
		attrAchievementRoleID.Int(int(a.RoleID)),
	)
	if a.PersonID.Valid {
		span.SetAttributes(attrPersonID.Int(int(a.PersonID.V)))
	}
	if a.TeamID.Valid {
		span.SetAttributes(attrTeamID.Int(int(a.TeamID.V)))
	}
}

func (a *Achievement) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, k []byte) (err error) {
		switch string(k) {
		case "id":
			a.ID, err = d.Int32()
		case "created_at":
			a.CreatedAt, err = DecodeTimestamp(d)
		case "updated_at":
			a.UpdatedAt, err = DecodeTimestamp(d)
		case "status":
			a.Status, err = d.Str()
		case "event_id":
			a.EventID, err = d.Int32()
		case "role_id":
			a.RoleID, err = d.Int32()
		case "person_id":
			err = a.PersonID.DecodeValue(d, DecodeInt32)
		case "team_id":
			err = a.TeamID.DecodeValue(d, DecodeInt32)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(k))
		}
		return nil
	})
}
