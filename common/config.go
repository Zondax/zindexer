package common

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

type DBConnectionConfig struct {
	GormConfig gorm.Config
}

func (p *DBConnectionParams) GetDSN() (string, error) {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		p.User, p.Password, p.Name, p.Host, p.Port), nil
}

func ConnectDB(params DBConnectionParams, config DBConnectionConfig) (*gorm.DB, error) {
	conn, err := NewPostgresConnection(&params, &config)
	if err != nil {
		return nil, err
	}

	return conn.GetDB(), nil
}
