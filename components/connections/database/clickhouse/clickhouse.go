package clickhouse

import (
	"fmt"
	"github.com/Zondax/zindexer/components/connections/database"
	"github.com/spf13/viper"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"strings"
)

type GormConnection struct {
	db *gorm.DB
}

type DBConnectionConfig struct {
	Gorm       *gorm.Config
	ClickHouse *clickhouse.Config
}

func GetTableName(table string) string {
	dbSchema := viper.GetString("db_schema")
	return fmt.Sprintf("%s.%s", dbSchema, table)
}

func GetTableNameWithoutSchema(fullName string) string {
	split := strings.Split(fullName, ".")
	return split[len(split)-1]
}

func NewClickHouseConnection(params *database.DBConnectionParams, config DBConnectionConfig) (*GormConnection, error) {
	if params.URI == "" {
		params.URI = fmt.Sprintf(
			"clickhouse://%s:%s@%s:%s/%s",
			params.User, params.Password, params.Host, params.Port, params.Name)

		if params.Params != "" {
			params.URI = fmt.Sprintf("%s?%s", params.URI, params.Params)
		}
	}

	var dbConfig gorm.Config
	if config.Gorm != nil {
		dbConfig = *config.Gorm
	}

	var clickhouseConfig clickhouse.Config
	if config.ClickHouse != nil {
		clickhouseConfig = *config.ClickHouse
	}

	if clickhouseConfig.DSN == "" {
		clickhouseConfig.DSN = params.URI
	}

	c, err := gorm.Open(clickhouse.New(clickhouseConfig), &dbConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to dial connect to db '%s@%s:%s': %v", params.Name, params.Host, params.Port, err)
	}

	return &GormConnection{db: c}, nil
}

func (c *GormConnection) GetDB() *gorm.DB {
	return c.db
}

func Connect(params database.DBConnectionParams, config DBConnectionConfig) (*gorm.DB, error) {
	conn, err := NewClickHouseConnection(&params, config)
	if err != nil {
		return nil, err
	}

	return conn.GetDB(), nil
}
