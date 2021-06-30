package WorkQueue

import cn "github.com/Zondax/zindexer/connections"

type IQueuedWorker interface {
	Start()
	DoWork(Work)
}

type QueuedWorker struct {
	Worker IQueuedWorker
}

type WorkerConstructor func(string, cn.DataTransport, chan chan Work) QueuedWorker

type Work struct {
	JobId  int64
	Params interface{}
}

type WorkQueue struct {
	ID          string
	WorkersChan chan chan Work // used to communicate between dispatcher and workers
	JobsChan    chan Work
	End         chan bool
}

func (w WorkQueue) ListenForJobs(cb func(Work)) {
	go func() {
		for {
			w.WorkersChan <- w.JobsChan
			select {
			case job := <-w.JobsChan:
				cb(job)
			case <-w.End:
				return
			}
		}
	}()
}
