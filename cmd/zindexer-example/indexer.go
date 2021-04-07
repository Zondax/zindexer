package main

import (
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

type ExampleIndexer struct {
	dbConn *gorm.DB
}

type ExampleData struct {
	Counter int64
}

func NewExampleIndexer(dbConn *gorm.DB) ExampleIndexer {
	return ExampleIndexer{
		dbConn: dbConn,
	}
}

func (indexer *ExampleIndexer) MigrateTypes() error {
	typesToMigrate := []interface{}{
		&ExampleData{},
	}

	zap.S().Infof("Migrating types")

	// Reference: https://gorm.io/docs/transactions.html#Transaction
	db := indexer.dbConn
	return db.Transaction(func(tx *gorm.DB) error {
		for _, t := range typesToMigrate {
			err := indexer.dbConn.AutoMigrate(t)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (indexer *ExampleIndexer) Start() error {
	err := indexer.MigrateTypes()
	if err != nil {
		return err
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
