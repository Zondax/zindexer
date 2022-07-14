package WorkQueue

import (
	"fmt"
	"go.uber.org/zap"
	"time"
)

type JobDispatcher struct {
	retryTimeout   time.Duration
	jobPool        *IndexJobPool
	endChan        chan bool          // channel to spin down workers
	workerChan     chan chan Job      // channel to send work to workers
	EmptyQueueChan chan bool          // channel to communicate that queue was consumed
	constructorFn  *WorkerConstructor // constructor fn for workers
}

func NewJobDispatcher(cfg DispatcherConfig) *JobDispatcher {
	d := JobDispatcher{
		retryTimeout:   cfg.RetryTimeout,
		jobPool:        NewJobPool(),
		workerChan:     make(chan chan Job),
		endChan:        make(chan bool),
		EmptyQueueChan: make(chan bool),
		constructorFn:  nil,
	}

	return &d
}

func (j *JobDispatcher) SetRetryTimeout(timeout time.Duration) {
	j.retryTimeout = timeout
}

func (j *JobDispatcher) SetWorkerConstructor(w *WorkerConstructor) {
	j.constructorFn = w
}

func (j *JobDispatcher) BuildWorkers(count int) {
	if j.constructorFn == nil {
		zap.S().Errorf("Cannot build workers: constructor function is nil!. Call SetWorkerConstructor first")
		return
	}
	zap.S().Infof("Spawning %d workers...", count)
	for i := 0; i < count; i++ {
		workerId := fmt.Sprintf("worker.%d", i)
		worker := (*j.constructorFn)(workerId, j.workerChan)
		worker.Worker.Start()
	}
}

func (j *JobDispatcher) Stop() {
	j.endChan <- true
}

func (j *JobDispatcher) Start() {
	go func() {
		for {
			job := j.jobPool.GetNewJob()
			if job.JobId == -1 {
				zap.S().Infof("*** No more jobs on JobPool, waiting.... ***")
				j.EmptyQueueChan <- true
				time.Sleep(j.retryTimeout)
				continue
			}

			select {
			case worker := <-j.workerChan: // wait for available channel
				worker <- job // dispatch job to worker
			case <-j.endChan:
				zap.S().Info("[JobDispatcher]- Received endChan")
				return
			}
		}
	}()
}

func (j *JobDispatcher) EnqueueJob(w Job) {
	j.jobPool.EnqueueJob(w)
}

func (j *JobDispatcher) EnqueueJobList(w *[]Job) {
	j.jobPool.EnqueueJobList(w)
}
