package main

import (
	"github.com/Zondax/zindexer/common"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start indexing",
	Run:   start,
}

func start(cmd *cobra.Command, args []string) {
	// Retrieve configuration
	var connParams common.DBConnectionParams
	err := viper.UnmarshalKey("db", &connParams)
	if err != nil {
		zap.S().Infof("error opening config file: %s", err)
	}

	dbConnConfig := common.DBConnectionConfig{
		GormConfig: gorm.Config{},
	}

	// Connect to database
	dbConn, err := common.ConnectDB(connParams, dbConnConfig)
	if err != nil {
		zap.S().Fatalf(err.Error())
	}

	// Create indexer
	exampleIndexer := NewExampleIndexer(dbConn)
	if err != nil {
		zap.S().Fatalf(err.Error())
	}

	zap.S().Infof("Starting Indexer")
	err = exampleIndexer.Start()
	if err != nil {
		zap.S().Fatalf(err.Error())
	}
}
