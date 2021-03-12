package zindexer

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type GormConnection struct {
	db *gorm.DB
}

func NewPostgresConnection(params *DBConnectionParams) (*GormConnection, error) {
	dsn, err := params.GetDSN()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve dsn")
	}

	conn, err := gorm.Open(postgres.Open(dsn), config.gorm)

	if err != nil {
		return nil, fmt.Errorf("failed to dial connect to db '%s@%s:%s': %v", params.Name, params.Host, params.Port, err)
	}

	return &GormConnection{
		db: conn,
	}, nil
}

func (c *GormConnection) GetDB() *gorm.DB {
	return c.db
}
