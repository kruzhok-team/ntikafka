package ntikafka

import (
	"testing"

	"github.com/go-faster/jx"
)

var teamBody = `{"before":null,"after":{"id":42,"created_at":"2026-07-22T16:08:30.509340Z","updated_at":"2026-07-29T05:49:36.524391Z","title":"Моя супер команда","description":"Описание для моей супер команды","owner_id":2,"event_id":3809,"invite_code":"9d123as481dznl00642","project_id":null,"imported_at":null,"imported_by_id":null,"assignment_participation":true,"created_by_assignment":false,"contact_link":"","created_by":null},"source":{"version":"3.0.8.Final","connector":"postgresql","name":"test","ts_ms":1785437006065,"snapshot":"false","db":"test","sequence":"[\"268550376552\",\"268550376936\"]","ts_us":1785437006065528,"ts_ns":1785437006065528000,"schema":"public","table":"team","txId":1284596861,"lsn":268550376936,"xmin":null},"transaction":null,"op":"u","ts_ms":1785437006438,"ts_us":1785437006438023,"ts_ns":1785437006438023818}`

func TestTeam(t *testing.T) {
	d := jx.DecodeStr(teamBody)
	v := new(Value)
	if err := v.Decode(d); err != nil {
		t.Fatal(err)
	}
	if !v.Valid {
		t.Fatal("!v.Valid")
	}
	if v.After == nil {
		t.Fatal("v.After == nil")
	}

	d.ResetBytes(v.After)
	s := new(Team)
	if err := s.Decode(d); err != nil {
		t.Fatal(err)
	}
	t.Logf("Team: %+v", s)
}

var teamPersonBody = `{"before":null,"after":{"id":3645,"person_id":2,"team_id":42,"owner_accepted":"a","user_accepted":"a"},"source":{"version":"3.0.8.Final","connector":"postgresql","name":"test","ts_ms":1785437441477,"snapshot":"false","db":"test","sequence":"[\"268550594648\",\"268550594976\"]","ts_us":1785437441477689,"ts_ns":1785437441477689000,"schema":"public","table":"teamperson","txId":1284597092,"lsn":268550594976,"xmin":null},"transaction":null,"op":"u","ts_ms":1785437441490,"ts_us":1785437441490421,"ts_ns":1785437441490421635}`

func TestTeamPerson(t *testing.T) {
	d := jx.DecodeStr(teamPersonBody)
	v := new(Value)
	if err := v.Decode(d); err != nil {
		t.Fatal(err)
	}
	if !v.Valid {
		t.Fatal("!v.Valid")
	}
	if v.After == nil {
		t.Fatal("v.After == nil")
	}

	d.ResetBytes(v.After)
	s := new(TeamPerson)
	if err := s.Decode(d); err != nil {
		t.Fatal(err)
	}
	t.Logf("TeamPerson: %+v", s)
}
