package database

import (
	"fmt"
	"gorm.io/gorm"
)

type DBConnection interface {
	GetDB() *gorm.DB
}

type IDBQueryClient interface {
	Connect() error
	GetDB() interface{}
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
