package connections

import (
	"fmt"
	"github.com/Zondax/zindexer"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func NewPostgresConnection(params *zindexer.DBConnectionParams, config *zindexer.DBConnectionConfig) (*GormConnection, error) {
	dsn, err := params.GetDSN()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve dsn")
	}

	conn, err := gorm.Open(postgres.Open(dsn), config.Gorm)

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

func ConnectDB(params zindexer.DBConnectionParams, config zindexer.DBConnectionConfig) (*gorm.DB, error) {
	conn, err := NewPostgresConnection(&params, &config)
	if err != nil {
		return nil, err
	}

	return conn.GetDB(), nil
}
