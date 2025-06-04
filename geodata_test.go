package ntikafka

import (
	"testing"

	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
)

const venueGeodata = `{
	"venue_id":1466,
	"country":"Россия",
	"federal_district":"Уральский",
	"region_with_type":"Свердловская обл",
	"city":"Екатеринбург",
	"city_type_full":"город",
	"settlement":null,
	"settlement_type_full":null,
	"city_district_with_type":null,
	"street_with_type":"ул Малышева",
	"house":"51",
	"floor":null,
	"address":"620000, Свердловская обл, г Екатеринбург, ул Малышева, д 51",
	"coordinates":{
		"x":60.614637,
		"y":56.836094,
		"wkb":"AQEAAACu9NpsrE5OQOAw0SAFa0xA",
		"srid":null
	}
}`

func TestVenueGeodata_Decode(t *testing.T) {
	d := jx.Decode(nil, 512)
	tests := []struct {
		name    string
		input   string
		want    VenueGeodata
		wantErr bool
	}{
		{
			name:  "empty",
			input: "{}",
		},
		{
			name:  "ok",
			input: venueGeodata,
			want: VenueGeodata{
				VenueID:         1466,
				Country:         "Россия",
				FederalDistrict: "Уральский",
				RegionWithType:  "Свердловская обл",
				City:            "Екатеринбург",
				CityTypeFull:    "город",
				StreetWithType:  "ул Малышева",
				House:           "51",
				Address:         "620000, Свердловская обл, г Екатеринбург, ул Малышева, д 51",
				Coordinates:     Point{X: 60.614637, Y: 56.836094, Valid: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d.ResetBytes([]byte(tt.input))
			var s VenueGeodata
			if err := s.Decode(d); err != nil {
				if !tt.wantErr {
					t.Errorf("Decode() failed: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Decode() succeeded unexpectedly")
			}
			if diff := cmp.Diff(tt.want, s); diff != "" {
				t.Errorf("VenueGeodata отличается от ожидаемого: %s", diff)
			}
		})
	}
}

func BenchmarkVenueGeodata_Decode(b *testing.B) {
	data := []byte(venueGeodata)
	d := jx.DecodeBytes(data)
	s := new(VenueGeodata)
	b.ReportAllocs()
	for b.Loop() {
		d.ResetBytes(data)
		if err := s.Decode(d); err != nil {
			b.Fatal(err)
		}
	}
}
