package WorkQueue

import (
	"fmt"
	"github.com/Zondax/zindexer/connections"
	"go.uber.org/zap"
	"time"
)

type JobDispatcher struct {
	jobPool       *IndexJobPool
	input         chan Work // channel to receive work
	end           chan bool // channel to spin down workers
	workerChannel chan chan Work
}

type DispatcherConfig struct {
	StartIndex int64
	EndIndex   int64
	JobsTopic  string
}

func NewJobDispatcher(cfg DispatcherConfig) JobDispatcher {
	d := JobDispatcher{
		jobPool: NewJobPool(PoolConfig{
			StartHeight: cfg.StartIndex,
			EndHeight:   cfg.EndIndex,
		}),
		workerChannel: make(chan chan Work),
		input:         make(chan Work),
		end:           make(chan bool),
	}

	return d
}

func (j JobDispatcher) BuildWorkers(count int, dataSource connections.DataSource, constructor WorkerConstructor) {
	for i := 0; i < count; i++ {
		workerId := fmt.Sprintf("worker.%d", i)
		worker := constructor(workerId, dataSource, j.workerChannel)
		worker.Worker.Start()
	}
}

// TODO listen for incoming jobs on JobsTopic
func (j JobDispatcher) Start() {
	j.dispatch()
	for {
		work := j.jobPool.GetNewJob()
		if work.JobId == -1 {
			zap.S().Infof("*** No more jobs on JobPool, waiting.... ***")
			time.Sleep(60 * time.Second)
			continue
		}

		j.input <- work
	}
}

func (j JobDispatcher) dispatch() {
	go func() {
		for {
			select {
			case <-j.end:
				fmt.Println("JobDispatcher received 'end'")
				return
			case work := <-j.input:
				worker := <-j.workerChannel // wait for available channel
				worker <- work              // dispatch work to worker
			}
		}
	}()
}
