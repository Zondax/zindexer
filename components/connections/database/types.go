package database

import (
	"fmt"
	"net/http"
)

type AuthHeaderTransport struct {
	Transport http.RoundTripper
	Token     string
}

func (adt AuthHeaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("x-hasura-admin-secret", adt.Token)
	return adt.Transport.RoundTrip(req)
}

type DBConnectionParams struct {
	User     string
	Password string
	Name     string
	Host     string
	Port     string
	Params   string
	URI      string
}

type GraphqlClientParams struct {
	Host  string
	Token string
}

func (p *DBConnectionParams) GetDSN() (string, error) {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		p.User, p.Password, p.Name, p.Host, p.Port), nil
}

func (p *DBConnectionParams) GetClickHouseDSN() (string, error) {
	return fmt.Sprintf(
		"clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s",
		p.User, p.Password, p.Host, p.Port, p.Name), nil
}
