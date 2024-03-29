package tests

import (
	"fmt"
	"github.com/Zondax/zindexer"
	"github.com/Zondax/zindexer/components/tracker"
	"github.com/Zondax/zindexer/indexer/tests/utils"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
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

func TestBasicIndexer(t *testing.T) {
	dbConn := utils.InitdbConn()
	setupTestingDB(dbConn)
	err := dbConn.AutoMigrate(DummyBlock{})
	if err != nil {
		panic(err)
	}

	// Create the indexer
	zidx := NewMockIndexer(dbConn, MockId, 100, 0)

	// Set the cb function that will be called when a buffer's sync event triggers
	zidx.BaseIndexer.SetSyncCB(zidx.MockSyncToDB)

	// Set up workers
	zidx.BaseIndexer.SetWorkerConstructor(zidx.NewMockWorker)
	zidx.BaseIndexer.BuildWorkers(20)

	// Set the function which retrieves missing heights
	zidx.BaseIndexer.SetGetMissingHeightsFn(zidx.MockGetMissingJobs)

	go zidx.BaseIndexer.StartIndexing()

	for {
		time.Sleep(30 * time.Second)
		heights, err := tracker.GetTrackedHeights(MockId, dbConn)
		if err != nil {
			return
		}

		if len(*heights) > 200 {
			fmt.Println("Test reached finish line without duplicates!. Rows inserted:", len(*heights))
			zidx.BaseIndexer.StopIndexing()
			break
		}
	}
}

func TestBasicIndexerWithExit(t *testing.T) {
	dbConn := utils.InitdbConn()
	setupTestingDB(dbConn)
	err := dbConn.AutoMigrate(DummyBlock{})
	if err != nil {
		panic(err)
	}

	// Create the indexer
	zidx := NewMockIndexer(dbConn, MockId, 100, 0)

	// Set the cb function that will be called when a buffer's sync event triggers
	zidx.BaseIndexer.SetSyncCB(zidx.MockSyncToDBWithExit)

	// Set up workers
	zidx.BaseIndexer.SetWorkerConstructor(zidx.NewMockWorker)
	zidx.BaseIndexer.BuildWorkers(1)

	// Set the function which retrieves missing heights
	zidx.BaseIndexer.SetGetMissingHeightsFn(zidx.MockGetMissingJobs)

	// Set test timeout
	go func() {
		time.Sleep(60 * time.Second)
		t.Error("Test timeout")
	}()

	go func() {
		<-zidx.dbSyncChan
		zap.S().Infof("dbSyncChan received... Stopping indexer")
		zidx.BaseIndexer.StopIndexing()
	}()

	zidx.BaseIndexer.StartIndexing()

	// Check test results
	heights, err := tracker.GetTrackedHeights(MockId, dbConn)
	if err != nil {
		t.Error(err)
	}

	if len(*heights) != MockSyncBlockPeriod {
		t.Error("indexer did not stop properly!")
	}
}

func TestBasicIndexerWithExitMultiWorkers(t *testing.T) {
	dbConn := utils.InitdbConn()
	setupTestingDB(dbConn)
	err := dbConn.AutoMigrate(DummyBlock{})
	if err != nil {
		panic(err)
	}

	// Create the indexer
	zidx := NewMockIndexer(dbConn, MockId, 100, 0)

	// Set the cb function that will be called when a buffer's sync event triggers
	zidx.BaseIndexer.SetSyncCB(zidx.MockSyncToDBWithExit)

	// Set up workers
	zidx.BaseIndexer.SetWorkerConstructor(zidx.NewMockWorker)
	zidx.BaseIndexer.BuildWorkers(20)

	// Set the function which retrieves missing heights
	zidx.BaseIndexer.SetGetMissingHeightsFn(zidx.MockGetMissingJobs)

	// Set test timeout
	go func() {
		time.Sleep(60 * time.Second)
		t.Error("Test timeout")
	}()

	go func() {
		<-zidx.dbSyncChan
		zap.S().Infof("dbSyncChan received... Stopping indexer")
		zidx.BaseIndexer.StopIndexing()
	}()

	zidx.BaseIndexer.StartIndexing()

	// Check test results
	heights, err := tracker.GetTrackedHeights(MockId, dbConn)
	if err != nil {
		t.Error(err)
	}

	if len(*heights) == 0 || len(*heights) > 2*MockSyncBlockPeriod {
		t.Error("indexer did not stop properly!")
	}
}
