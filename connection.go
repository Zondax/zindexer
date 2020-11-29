package zindexer

import (
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// FIXME: Do we really need this type?

type GormConnection struct {
	db *gorm.DB
}

func NewPostgresConnection(params *ConnectionParams) (*GormConnection, error) {
	log.Println("Creating connection with db")
	dsn, err := params.GetDSN()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve dsn")
	}

	conn, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
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
