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