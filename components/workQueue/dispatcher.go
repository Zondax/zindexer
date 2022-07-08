package WorkQueue

import (
	"fmt"
	"go.uber.org/zap"
	"time"
)

type JobDispatcher struct {
	retryTimeout   time.Duration
	jobPool        *IndexJobPool
	inputChan      chan Work          // channel to receive work
	endChan        chan bool          // channel to spin down workers
	workerChan     chan chan Work     // channel to send work to workers
	EmptyQueueChan chan bool          // channel to communicate that queue was consumed
	constructorFn  *WorkerConstructor // constructor fn for workers
}

func NewJobDispatcher(cfg DispatcherConfig) *JobDispatcher {
	d := JobDispatcher{
		retryTimeout:   cfg.RetryTimeout,
		jobPool:        NewJobPool(),
		workerChan:     make(chan chan Work),
		inputChan:      make(chan Work, 1),
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
			select {
			case <-j.endChan:
				zap.S().Info("[JobDispatcher]- Received endChan")
				return
			case work := <-j.inputChan:
				worker := <-j.workerChan // wait for available channel
				worker <- work           // dispatch work to worker
			default:
				work := j.jobPool.GetNewJob()
				if work.JobId == -1 {
					zap.S().Infof("*** No more jobs on JobPool, waiting.... ***")
					if len(j.inputChan) == 0 {
						j.EmptyQueueChan <- true
					}
					time.Sleep(j.retryTimeout)
					continue
				}

				j.inputChan <- work
			}
		}
	}()
}

func (j *JobDispatcher) EnqueueWork(w Work) {
	j.jobPool.EnqueueJob(w)
}

func (j *JobDispatcher) EnqueueWorkList(w *[]Work) {
	j.jobPool.EnqueueJobList(w)
}
