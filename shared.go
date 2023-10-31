package ntikafka

import (
	"crypto/tls"
	"crypto/x509"
	_ "embed"
)

//go:generate wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" --output-document yandexCA.pem
//go:embed yandexCA.pem
var yandexCA []byte

var tlscfg *tls.Config

func init() {
	certs := x509.NewCertPool()
	certs.AppendCertsFromPEM(yandexCA)
	tlscfg = &tls.Config{
		InsecureSkipVerify: true,
		RootCAs:            certs,
	}
}
