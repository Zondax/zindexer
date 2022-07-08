package tests

import (
	"fmt"
	"github.com/Zondax/zindexer/components/connections/database/postgres"
	"github.com/Zondax/zindexer/components/db_buffer"
	"github.com/Zondax/zindexer/components/tracker"
	"github.com/Zondax/zindexer/components/workQueue"
	"github.com/Zondax/zindexer/indexer"
	"github.com/Zondax/zindexer/indexer/tests/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"time"
)

const (
	MockSyncBlockPeriod = 10
	MockSyncTimePeriod  = 5 * time.Second
	MockId              = "test"
)

type DummyBlock struct {
	Height uint64 `gorm:"unique"`
	Hash   string
}

func (DummyBlock) TableName() string {
	return postgres.GetTableName("transactions")
}

type MockIndexer struct {
	BaseIndexer   *indexer.Indexer
	GenesisHeight uint64
	TipHeight     uint64
}

type MockWorker struct {
	workQueue WorkQueue.WorkQueue
	buffer    *db_buffer.Buffer
}

func NewMockIndexer(dbConn *gorm.DB, id string, tip, genesis uint64) *MockIndexer {
	config := indexer.Config{
		EnableBuffer: true,
		ComponentsCfg: indexer.ComponentsCfg{
			DBBufferCfg: db_buffer.Config{
				SyncTimePeriod:     MockSyncTimePeriod,
				SyncBlockThreshold: MockSyncBlockPeriod,
			},
			DispatcherCfg: WorkQueue.DispatcherConfig{
				RetryTimeout: MockSyncTimePeriod,
			},
		},
	}

	mockIndexer := MockIndexer{
		BaseIndexer:   indexer.NewIndexer(dbConn, id, config),
		TipHeight:     tip,
		GenesisHeight: genesis,
	}
	return &mockIndexer
}

func (i *MockIndexer) MockGetMissingHeights() (*[]uint64, error) {
	heights, err := tracker.GetMissingHeights(i.TipHeight, i.GenesisHeight, tracker.NoReturnLimit,
		i.BaseIndexer.Id, i.BaseIndexer.DbConn)
	if err != nil {
		return nil, err
	}

	i.TipHeight += uint64(utils.RandomInt64(10))

	return heights, nil
}

func (i *MockIndexer) MockSyncToDB() db_buffer.SyncResult {
	fmt.Println("Syncing to DB")

	data, err := i.BaseIndexer.DBBuffer.GetData("dummy")
	if err != nil {
		panic(err)
	}

	if len(data) == 0 {
		return db_buffer.SyncResult{}
	}

	var dummyBlocks []DummyBlock
	var heights []uint64
	for i, b := range data {
		block := b.(DummyBlock)
		height, _ := strconv.Atoi(i)
		dummyBlocks = append(dummyBlocks, block)
		heights = append(heights, uint64(height))
	}

	// Will panic if tries to insert a duplicate
	tx := i.BaseIndexer.DbConn.Create(dummyBlocks)
	if tx.Error != nil {
		panic(tx.Error)
	}

	return db_buffer.SyncResult{
		Id:            MockId,
		SyncedHeights: &heights,
	}
}

func (i *MockIndexer) MockSyncToDBWithExit() db_buffer.SyncResult {
	res := i.MockSyncToDB()

	// trigger onExit call
	i.BaseIndexer.StopIndexing()
	time.Sleep(10 * time.Second)

	return res
}

func (i *MockIndexer) NewMockWorker(id string, workerChannel chan chan WorkQueue.Work) WorkQueue.QueuedWorker {
	worker := MockWorker{
		buffer: i.BaseIndexer.DBBuffer,
		workQueue: WorkQueue.WorkQueue{
			ID:          id,
			WorkersChan: workerChannel,
			JobsChan:    make(chan WorkQueue.Work),
			End:         make(chan bool),
		},
	}

	fmt.Println("Created mock worker with id:", id)
	return WorkQueue.QueuedWorker{Worker: &worker}
}

func (m *MockWorker) Start() {
	m.workQueue.ListenForJobs(m.DoWork)
	zap.S().Infof("Worker %s listening for jobs", m.workQueue.ID)
}

func (m *MockWorker) DoWork(w WorkQueue.Work) {
	fmt.Println("Worker received work id", w.JobId)
	data := DummyBlock{
		Height: uint64(w.JobId),
		Hash:   utils.NewSHA256Hash(),
	}

	err := m.buffer.InsertData("dummy", w.JobId, data, true)
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Worker finished work id", w.JobId)
}
