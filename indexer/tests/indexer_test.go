package tests

import (
	"fmt"
	"github.com/Zondax/zindexer"
	"github.com/Zondax/zindexer/components/tracker"
	"github.com/Zondax/zindexer/indexer/tests/utils"
	"github.com/spf13/viper"
	"gorm.io/gorm"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	viper.SetDefault("db_schema", "testing")
	zindexer.InitGlobalLogger()
	c := m.Run()
	os.Exit(c)
}

func setupTestingDB(db *gorm.DB) {
	err := db.Transaction(func(sqlTx *gorm.DB) error {
		sqlTx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tracker.DbSection{}.TableName()))
		sqlTx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", DummyBlock{}.TableName()))
		err := sqlTx.AutoMigrate(tracker.DbSection{}, DummyBlock{})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func Test_BasicIndexer(t *testing.T) {
	dbConn := utils.InitdbConn()
	setupTestingDB(dbConn)
	err := dbConn.AutoMigrate(DummyBlock{})
	if err != nil {
		panic(err)
	}

	// Create the indexer
	zidx := NewMockIndexer(dbConn, "test", 1000, 0)

	// Set the cb function that will be called when a buffer's sync event triggers
	zidx.BaseIndexer.SetSyncCB(zidx.MockSyncToDB)

	// Set up workers
	zidx.BaseIndexer.SetWorkerConstructor(zidx.NewMockWorker)
	zidx.BaseIndexer.BuildWorkers(20)

	// Set the function which retrieves missing heights
	zidx.BaseIndexer.SetGetMissingHeightsFn(zidx.MockGetMissingHeights)

	// Start indexing (blocking)
	err = zidx.BaseIndexer.StartIndexing()
	if err != nil {
		panic(err)
	}
}
