package tests

import (
	"github.com/Zondax/zindexer"
	"github.com/Zondax/zindexer/indexer/tests/utils"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	zindexer.InitGlobalLogger()
	c := m.Run()
	os.Exit(c)
}

func Test_BasicIndexer(t *testing.T) {
	dbConn := utils.InitdbConn()
	err := dbConn.AutoMigrate(DummyBlock{})
	if err != nil {
		panic(err)
	}

	zidx := NewMockIndexer(dbConn, "test")

	// Set the cb function that will be called when a buffer's sync event triggers
	zidx.BaseIndexer.SetSyncCB(zidx.MockSyncToDB)

	// Set up the workers
	zidx.BaseIndexer.SetWorkerConstructor(zidx.NewMockWorker)
	zidx.BaseIndexer.BuildWorkers(10)

	// Set the function which retrieves missing heights
	zidx.BaseIndexer.SetGetMissingHeightsFn(zidx.MockGetMissingHeights)

	// Start indexing (blocking)
	err = zidx.BaseIndexer.StartIndexing()
	if err != nil {
		panic(err)
	}
}
