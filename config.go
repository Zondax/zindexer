package zindexer

import (
	"fmt"
	"gorm.io/gorm"
)

type DBConnectionParams struct {
	User     string
	Password string
	Name     string
	Host     string
	Port     string
}

type DBSubscriptionParams struct {
	Host  string
	Token string
}

type DBConnectionConfig struct {
	Gorm *gorm.Config
}

func (p *DBConnectionParams) GetDSN() (string, error) {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		p.User, p.Password, p.Name, p.Host, p.Port), nil
}
