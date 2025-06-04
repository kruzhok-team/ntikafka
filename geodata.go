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
}

func (s *VenueGeodata) SetAttributes(span trace.Span) {
	span.SetAttributes(attrVenueID.Int(int(s.VenueID)))
}

func (s *VenueGeodata) Decode(d *jx.Decoder) error {
	return d.ObjBytes(func(d *jx.Decoder, key []byte) error {
		var err error
		switch string(key) {
		case "venue_id":
			s.VenueID, err = d.Int32()
		case "country":
			s.Country, err = deNullStr(d)
		case "federal_district":
			s.FederalDistrict, err = deNullStr(d)
		case "region_with_type":
			s.RegionWithType, err = deNullStr(d)
		case "city":
			s.City, err = deNullStr(d)
		case "city_type_full":
			s.CityTypeFull, err = deNullStr(d)
		case "settlement":
			s.Settlement, err = deNullStr(d)
		case "settlement_type_full":
			s.SettlementTypeFull, err = deNullStr(d)
		case "city_district_with_type":
			s.CityDistrictWithType, err = deNullStr(d)
		case "street_with_type":
			s.StreetWithType, err = deNullStr(d)
		case "house":
			s.House, err = deNullStr(d)
		case "floor":
			s.Floor, err = deNullStr(d)
		case "address":
			s.Address, err = deNullStr(d)
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
