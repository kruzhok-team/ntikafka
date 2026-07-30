package ntikafka

import (
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type Team struct {
	ID                      int32
	CreatedAt               time.Time
	UpdatedAt               time.Time
	ImportedAt              Null[time.Time]
	OwnerID                 Null[int32]
	EventID                 Null[int32]
	ProjectID               Null[int32]
	CreatedBy               Null[int32]
	Title                   string
	Description             string
	ContactLink             string
	InviteCode              string
	AssignmentParticipation bool
	CreatedByAssignment     bool
}

func (t *Team) SetAttributes(span trace.Span) {
	span.SetAttributes(attrTeamID.Int(int(t.ID)))
	if t.EventID.Valid {
		span.SetAttributes(attrEventID.Int(int(t.EventID.V)))
	}
}

func (t *Team) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, k []byte) (err error) {
		switch string(k) {
		case "id":
			t.ID, err = d.Int32()
		case "created_at":
			t.CreatedAt, err = DecodeTimestamp(d)
		case "updated_at":
			t.UpdatedAt, err = DecodeTimestamp(d)
		case "title":
			t.Title, err = d.Str()
		case "description":
			t.Description, err = d.Str()
		case "owner_id":
			err = t.OwnerID.DecodeValue(d, DecodeInt32)
		case "event_id":
			err = t.EventID.DecodeValue(d, DecodeInt32)
		case "project_id":
			err = t.ProjectID.DecodeValue(d, DecodeInt32)
		case "created_by":
			err = t.CreatedBy.DecodeValue(d, DecodeInt32)
		case "imported_at":
			err = t.ImportedAt.DecodeValue(d, DecodeTimestamp)
		case "assignment_participation":
			t.AssignmentParticipation, err = d.Bool()
		case "created_by_assignment":
			t.CreatedByAssignment, err = d.Bool()
		case "contact_link":
			t.ContactLink, err = d.Str()
		case "invite_code":
			t.InviteCode, err = d.Str()
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(k))
		}
		return nil
	})
}

type TeamPerson struct {
	ID            int32
	PersonID      int32
	TeamID        int32
	OwnerAccepted string
	UserAccepted  string
}

func (p *TeamPerson) SetAttributes(span trace.Span) {
	span.SetAttributes(
		attrTeamID.Int(int(p.TeamID)),
		attrPersonID.Int(int(p.PersonID)),
		attrTeamPersonID.Int(int(p.ID)),
	)
}

func (p *TeamPerson) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, k []byte) (err error) {
		switch string(k) {
		case "id":
			p.ID, err = d.Int32()
		case "person_id":
			p.PersonID, err = d.Int32()
		case "team_id":
			p.TeamID, err = d.Int32()
		case "owner_accepted":
			p.OwnerAccepted, err = d.Str()
		case "user_accepted":
			p.UserAccepted, err = d.Str()
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(k))
		}
		return nil
	})
}
