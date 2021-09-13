package database

import (
	"fmt"
	"net/http"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
)

type AuthHeaderTransport struct {
	Transport http.RoundTripper
	Token     string
}

func (adt AuthHeaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("x-hasura-admin-secret", adt.Token)
	return adt.Transport.RoundTrip(req)
}

type DBConnection interface {
	GetDB() *gorm.DB
}

type IDBQueryClient interface {
	GetDB() interface{}
	Connect() error
	GetMongoDoc(collection *mongo.Collection, docId string) (bson.M, error)
}

type DBQueryClient struct {
	Client IDBQueryClient
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
