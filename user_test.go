package ntikafka

import (
	"testing"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
)

func TestUser_Decode(t *testing.T) {
	var got User
	err := got.Decode(jx.DecodeBytes([]byte(`{
"id": 123,
"is_active": true,
"email": "a@b.c",
"phone": "+7 999 555-44-33",
"phone_confirmed": true,
"first_name": "Антон",
"last_name": "Антонов",
"middle_name": "Антонович",
"no_middle_name": false,
"date_joined": "2020-09-01T14:51:43.000000Z",
"sex": "m",
"birthday": 6822,
"last_login": "2026-05-24T11:27:13.976619Z"
	}`)))
	if err != nil {
		t.Fatal(err)
	}
	want := User{
		ID:             123,
		Active:         true,
		Email:          "a@b.c",
		Phone:          "+7 999 555-44-33",
		PhoneConfirmed: true,
		LastName:       "Антонов",
		FirstName:      "Антон",
		MiddleName:     "Антонович",
		NoMiddleName:   false,
		DateJoined:     time.Date(2020, time.September, 01, 14, 51, 43, 0, time.UTC),
		Sex:            Null[string]{V: "m", Valid: true},
		Birthday:       Null[time.Time]{V: time.Date(1988, time.September, 5, 0, 0, 0, 0, time.UTC), Valid: true},
		LastLogin:      Null[time.Time]{V: time.Date(2026, time.May, 24, 11, 27, 13, 976619000, time.UTC), Valid: true},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("User.Decode:\n%s", diff)
	}
}
