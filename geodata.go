package ntikafka

import (
	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"go.opentelemetry.io/otel/trace"
)

type VenueGeodata struct {
	VenueID              int32
	Country              string
	FederalDistrict      string
	RegionWithType       string
	City                 string
	CityTypeFull         string
	Settlement           string
	SettlementTypeFull   string
	CityDistrictWithType string
	StreetWithType       string
	House                string
	Floor                string
	Address              string
	Coordinates          Point

	Valid   bool
	Payload Value
}

func (s *VenueGeodata) SetAttributes(span trace.Span) {
	span.SetAttributes(attrVenueID.Int(int(s.VenueID)))
}

func (s *VenueGeodata) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		s.Valid = true
		var err error
		switch string(key) {
		case "venue_id":
			s.VenueID, err = d.Int32()
		case "country":
			s.Country, err = DeNullStr(d)
		case "federal_district":
			s.FederalDistrict, err = DeNullStr(d)
		case "region_with_type":
			s.RegionWithType, err = DeNullStr(d)
		case "city":
			s.City, err = DeNullStr(d)
		case "city_type_full":
			s.CityTypeFull, err = DeNullStr(d)
		case "settlement":
			s.Settlement, err = DeNullStr(d)
		case "settlement_type_full":
			s.SettlementTypeFull, err = DeNullStr(d)
		case "city_district_with_type":
			s.CityDistrictWithType, err = DeNullStr(d)
		case "house":
			s.House, err = DeNullStr(d)
		case "floor":
			s.Floor, err = DeNullStr(d)
		case "address":
			s.Address, err = DeNullStr(d)
		case "coordinates":
			err = s.Coordinates.Decode(d)
		default:
			err = d.Skip()
		}
		if err != nil {
			err = errors.Wrap(err, string(key))
		}
		return err
	})
}
