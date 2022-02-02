package indexer

import (
	"github.com/Zondax/zindexer/db_buffer"
	"github.com/Zondax/zindexer/tracker"
	WorkQueue "github.com/Zondax/zindexer/workers/workQueue"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"os/signal"
)

type MissingHeightsFn func() (*[]uint64, error)

type Indexer struct {
	Id               string
	DbConn           *gorm.DB
	DBBuffer         *db_buffer.Buffer
	jobDispatcher    *WorkQueue.JobDispatcher
	missingHeightsCB MissingHeightsFn

	stopChan chan bool
}

func NewIndexer(dbConn *gorm.DB, id string, cfg Config) *Indexer {
	dbBuffer := db_buffer.NewDBBuffer(dbConn, cfg.DBBufferCfg)
	dispatcher := WorkQueue.NewJobDispatcher(cfg.DispatcherCfg)

	return &Indexer{
		Id:            id,
		DbConn:        dbConn,
		DBBuffer:      dbBuffer,
		jobDispatcher: dispatcher,
		stopChan:      make(chan bool),
	}
}

func (i *Indexer) SetWorkerConstructor(w WorkQueue.WorkerConstructor) {
	if w == nil {
		zap.S().Errorf("worker constructor cannot be nil")
		return
	}
	i.jobDispatcher.SetWorkerConstructor(&w)
}

func (i *Indexer) BuildWorkers(c int) {
	i.jobDispatcher.BuildWorkers(c)
}

func (i *Indexer) SetSyncCB(cb db_buffer.SyncCB) {
	i.DBBuffer.SetSyncFunc(cb)
}

func (i *Indexer) SetGetMissingHeightsFn(fn MissingHeightsFn) {
	i.missingHeightsCB = fn
}

func (i *Indexer) StartIndexing() error {
	// Clear all in-progress jobs of previous run
	err := tracker.ClearInProgress(i.Id, i.DbConn)
	if err != nil {
		zap.S().Error(err)
		panic(err)
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt)

	//Start db_buffer
	i.DBBuffer.Start()

	//Start job dispatcher
	i.jobDispatcher.Start()

	// Main loop
	for {
		select {
		case <-i.jobDispatcher.EmptyQueueChan:
			i.onJobQueueEmpty()
		case r := <-i.DBBuffer.SyncComplete:
			i.onDBSyncComplete(r)
		case <-exitChan:
			zap.S().Infof("*** Indexer '%s' exited by system abort ***", i.Id)
			return nil
		case <-i.stopChan:
			zap.S().Infof("*** Indexer '%s' exited by stop signal ***", i.Id)
			return nil
		}
	}
}

func (i *Indexer) StopIndexing() {
	i.stopChan <- true
}

func (i *Indexer) addPendingHeights(p *[]uint64) error {
	pendingJobs := make([]WorkQueue.Work, len(*p))
	for i, h := range *p {
		pendingJobs[i] = WorkQueue.Work{JobId: int64(h)}
	}

	// Mark pending jobs as WIP in tracking table
	err := tracker.UpdateInProgressHeight(true, p, i.Id, i.DbConn)
	if err != nil {
		return err
	}

	// Enqueue jobs
	i.jobDispatcher.EnqueueWorkList(&pendingJobs)
	return nil
}

func (i *Indexer) onDBSyncComplete(r db_buffer.SyncResult) {
	if r.Error != nil {
		zap.S().Errorf(r.Error.Error())
	}

	if r.SyncedHeights == nil {
		return
	}

	err := tracker.UpdateAndRemoveWipHeights(r.SyncedHeights, i.Id, i.DbConn)
	if err != nil {
		return
	}
}

func (i *Indexer) onJobQueueEmpty() {
	pendingHeights, err := i.missingHeightsCB()
	if err != nil || pendingHeights == nil {
		zap.S().Infof("pending blocks list is empty, retrying...")
		return
	}
	zap.S().Infof("Got %d pending heights", len(*pendingHeights))
	err = i.addPendingHeights(pendingHeights)
	if err != nil {
		zap.S().Errorf(err.Error())
	}
}