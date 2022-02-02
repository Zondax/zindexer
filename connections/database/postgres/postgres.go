package postgres

import (
	"fmt"
	"github.com/Zondax/zindexer/connections/database"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"strings"
)

type GormConnection struct {
	db *gorm.DB
}

type DBConnectionConfig struct {
	Gorm *gorm.Config
}

func GetTableName(table string) string {
	dbSchema := viper.GetString("db_schema")
	return fmt.Sprintf("%s.%s", dbSchema, table)
}

func GetTableNameWithoutSchema(fullName string) string {
	split := strings.Split(fullName, ".")
	return split[len(split)-1]
}

func NewPostgresConnection(params *database.DBConnectionParams, config DBConnectionConfig) (*GormConnection, error) {
	dsn, err := params.GetDSN()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve dsn")
	}

	var dbConfig gorm.Config
	if config.Gorm != nil {
		dbConfig = *config.Gorm
	}
	c, err := gorm.Open(postgres.Open(dsn), &dbConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to dial connect to db '%s@%s:%s': %v", params.Name, params.Host, params.Port, err)
	}

	return &GormConnection{db: c}, nil
}

func (c *GormConnection) GetDB() *gorm.DB {
	return c.db
}

func Connect(params database.DBConnectionParams, config DBConnectionConfig) (*gorm.DB, error) {
	conn, err := NewPostgresConnection(&params, config)
	if err != nil {
		return nil, err
	}

	return conn.GetDB(), nil
}
