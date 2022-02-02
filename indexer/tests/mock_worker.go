package tests

import (
	"fmt"
	"github.com/Zondax/zindexer/db_buffer"
	"github.com/Zondax/zindexer/indexer"
	"github.com/Zondax/zindexer/indexer/tests/utils"
	"github.com/Zondax/zindexer/tracker"
	WorkQueue "github.com/Zondax/zindexer/workers/workQueue"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"time"
)

var MovingTip uint64 = 100

type DummyBlock struct {
	Height uint64
	Hash   string
}

func (DummyBlock) TableName() string {
	return "testing.transactions" //TODO
}

type MockIndexer struct {
	BaseIndexer *indexer.Indexer
}

type MockWorker struct {
	workQueue   WorkQueue.WorkQueue
	currentWork WorkQueue.Work

	//
	buffer *db_buffer.Buffer
}

func NewMockIndexer(dbConn *gorm.DB, id string) *MockIndexer {
	config := indexer.Config{
		ComponentsCfg: indexer.ComponentsCfg{
			DBBufferCfg: db_buffer.Config{
				SyncTimePeriod:     5 * time.Second,
				SyncBlockThreshold: 10,
			},
			DispatcherCfg: WorkQueue.Config{
				RetryTimeout: 10 * time.Second,
			},
		},
	}

	mockIndexer := MockIndexer{BaseIndexer: indexer.NewIndexer(dbConn, id, config)}
	return &mockIndexer
}

func (i *MockIndexer) MockGetMissingHeights() (*[]uint64, error) {
	heights, err := tracker.GetMissingHeights(MovingTip, 0, tracker.NoReturnLimit,
		i.BaseIndexer.Id, i.BaseIndexer.DbConn)
	if err != nil {
		return nil, err
	}

	MovingTip++
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

	i.BaseIndexer.DbConn.Create(dummyBlocks)

	return db_buffer.SyncResult{
		SyncedHeights: &heights,
	}
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
		Hash:   utils.NewSHA1Hash(),
	}

	err := m.buffer.InsertData("dummy", w.JobId, data, true)
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Worker finished work id", w.JobId)
}
