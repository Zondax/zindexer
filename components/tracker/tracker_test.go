package tracker

import (
	"fmt"
	"github.com/Zondax/zindexer/components/connections/database"
	"github.com/Zondax/zindexer/components/connections/database/postgres"
	"reflect"
	"testing"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	testingId     = "testing"
	genesisHeight = 0
	tipHeight     = 10
	db_schema     = "testing"
)

var dbConn *gorm.DB = nil

func init() {
	if dbConn != nil {
		return
	}

	viper.SetDefault("db_schema", db_schema)
	connParams := database.DBConnectionParams{
		User:     "postgres",
		Password: "postgrespassword",
		Name:     "postgres",
		Host:     "db",
		Port:     "5432",
	}

	// Connect to database
	var err error
	dbConn, err = postgres.Connect(
		connParams,
		postgres.DBConnectionConfig{
			Gorm: &gorm.Config{
				Logger: logger.Default.LogMode(logger.Error),
			},
		},
	)
	if err != nil {
		zap.S().Fatalf(err.Error())
		panic(err)
	}

	// Try creating schema, fails if already exists
	tx := dbConn.Exec(fmt.Sprintf("CREATE SCHEMA %s", db_schema))
	if tx.Error != nil {
		fmt.Println(tx.Error)
	}

	err = dbConn.AutoMigrate(DbSection{})
	if err != nil {
		panic(err)
	}
}

func TestTracer_DBInsertRead(t *testing.T) {
	// Empty database table
	dbConn.Exec("DELETE from testing.tracking")

	trackedHeights := []uint64{1, 2, 4, 5, 6, 11, 12, 15, 16, 17, 20}
	err := UpdateTrackedHeights(&trackedHeights, testingId, dbConn)
	if err != nil {
		t.Errorf("Failed to update heights with error %v", err)
	}

	heights, err := GetTrackedHeights(testingId, dbConn)
	if err != nil {
		t.Errorf("Failed to update heights with error %v", err)
	}

	if !reflect.DeepEqual(*heights, trackedHeights) {
		t.Errorf("Heights inserted do not match. Wanted: %v, Got: %v", trackedHeights, *heights)
	}

	trackedHeights = append(trackedHeights, []uint64{40, 41}...)
	err = UpdateTrackedHeights(&trackedHeights, "testing", dbConn)
	if err != nil {
		t.Errorf("Failed to update heights with error %v", err)
	}

	heights, err = GetTrackedHeights("testing", dbConn)
	if err != nil {
		t.Errorf("Failed to update heights with error %v", err)
	}

	if !reflect.DeepEqual(*heights, trackedHeights) {
		t.Errorf("Heights inserted do not match. Wanted: %v, Got: %v", trackedHeights, *heights)
	}
}

func TestTracer_MissingHeights(t *testing.T) {
	// Empty database table
	dbConn.Exec("DELETE from testing.tracking")

	trackedHeights := &[]uint64{genesisHeight, tipHeight}
	inProgressHeights := []uint64{1, 2, 3}

	err := UpdateTrackedHeights(trackedHeights, testingId, dbConn)
	if err != nil {
		t.Errorf(err.Error())
	}

	var expectedMissing_1 = &[]uint64{9, 8, 7, 6, 5, 4, 3, 2, 1}
	missing, err := GetMissingHeights(tipHeight, genesisHeight, NoReturnLimit, testingId, dbConn)
	if err != nil {
		t.Errorf(err.Error())
	}

	if !reflect.DeepEqual(expectedMissing_1, missing) {
		t.Errorf("Missing heights do not match. Wanted: %v, Got: %v", expectedMissing_1, missing)
	}

	// Track some in-progress heights
	for _, height := range inProgressHeights {
		err := UpdateInProgressHeight(true, &[]uint64{height}, testingId, dbConn)
		if err != nil {
			zap.S().Errorf(err.Error())
		}
	}

	var expectedMissing_2 = &[]uint64{9, 8, 7, 6, 5, 4}
	missing, err = GetMissingHeights(tipHeight, genesisHeight, NoReturnLimit, testingId, dbConn)
	if err != nil {
		t.Errorf(err.Error())
	}

	if !reflect.DeepEqual(expectedMissing_2, missing) {
		t.Errorf("Missing heights do not match. Wanted: %v, Got: %v", expectedMissing_2, missing)
	}

	// Untrack the previous added in-progress heights
	for _, height := range inProgressHeights {
		err := UpdateInProgressHeight(false, &[]uint64{height}, testingId, dbConn)
		if err != nil {
			zap.S().Errorf(err.Error())
		}
	}

	var expectedMissing_3 = &[]uint64{9, 8, 7, 6, 5, 4, 3, 2, 1}
	missing, err = GetMissingHeights(tipHeight, genesisHeight, NoReturnLimit, testingId, dbConn)
	if err != nil {
		t.Errorf(err.Error())
	}

	if !reflect.DeepEqual(expectedMissing_3, missing) {
		t.Errorf("Missing heights do not match. Wanted: %v, Got: %v", expectedMissing_3, missing)
	}
}
