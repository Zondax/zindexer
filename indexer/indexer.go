package indexer

import (
	"github.com/Zondax/zindexer/components/db_buffer"
	"github.com/Zondax/zindexer/components/tracker"
	"github.com/Zondax/zindexer/components/workQueue"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type MissingHeightsFn func() (*[]uint64, error)

type Indexer struct {
	Id               string
	DbConn           *gorm.DB
	DBBuffer         *db_buffer.Buffer
	jobDispatcher    *WorkQueue.JobDispatcher
	missingHeightsCB MissingHeightsFn
	Config           Config

	stopReqChan  chan bool
	stopResChan  chan bool
	statusServer *StatusServer
}

func NewIndexer(dbConn *gorm.DB, id string, cfg Config) *Indexer {
	checkConfig(&cfg)

	dbBuffer := db_buffer.NewDBBuffer(dbConn, cfg.DBBufferCfg)
	dispatcher := WorkQueue.NewJobDispatcher(cfg.DispatcherCfg)

	return &Indexer{
		Id:            id,
		DbConn:        dbConn,
		DBBuffer:      dbBuffer,
		jobDispatcher: dispatcher,
		Config:        cfg,
		stopReqChan:   make(chan bool),
		stopResChan:   make(chan bool),
	}
}

func checkConfig(cfg *Config) {
	// buffer
	if cfg.DBBufferCfg.SyncTimePeriod <= 0 {
		zap.S().Debugf("Setting default value for DbBuffer SyncTimePeriod: %s", db_buffer.DefaultSyncPeriod.String())
		cfg.DBBufferCfg.SyncTimePeriod = db_buffer.DefaultSyncPeriod
	}

	// dispatcher
	if cfg.DispatcherCfg.RetryTimeout <= 0 {
		zap.S().Debugf("Setting default value for Dispatcher's DefaultRetryTimeout: %s", WorkQueue.DefaultRetryTimeout.String())
		cfg.DispatcherCfg.RetryTimeout = WorkQueue.DefaultRetryTimeout
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

func (i *Indexer) StartIndexing() {
	// Clear all in-progress jobs of previous run
	err := tracker.ClearInProgress(i.Id, i.DbConn)
	if err != nil {
		zap.S().Error(err)
		panic(err)
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Start db_buffer
	if i.Config.EnableBuffer {
		i.DBBuffer.Start()
	}

	// Start job dispatcher
	i.jobDispatcher.Start()

	// Status server
	i.statusServer = NewStatusServer(i)
	i.statusServer.Start()

	// Main loop
	for {
		select {
		case <-i.jobDispatcher.EmptyQueueChan:
			i.onJobQueueEmpty()
		case <-exitChan:
			zap.S().Debugf("Exit signal catched!")
			i.onStop()
			return
		case <-i.stopReqChan:
			zap.S().Debugf("Stop signal received!")
			i.onStop()
			// give some time to status server to be able to return the response
			time.Sleep(5 * time.Second)
			return
		}
	}
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

func (i *Indexer) StopIndexing() {
	zap.S().Info("[Indexer] - StopIndexing START")
	i.stopReqChan <- true
	<-i.stopResChan
	zap.S().Info("[Indexer] - StopIndexing END")
}

func (i *Indexer) onStop() {
	zap.S().Info("[Indexer]- graceful shutdown requested!")
	i.jobDispatcher.Stop()
	i.DBBuffer.Stop()
	i.stopResChan <- true
	zap.S().Info("[Indexer]- graceful shutdown done!")
}
