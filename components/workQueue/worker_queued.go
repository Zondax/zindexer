package WorkQueue

import "go.uber.org/zap"

type IQueuedWorker interface {
	Start()
	DoWork(Work)
}

type QueuedWorker struct {
	Worker IQueuedWorker
}

type WorkerConstructor func(string, chan chan Work) QueuedWorker

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

func (w WorkQueue) Stop() {
	w.End <- true
}

func (w WorkQueue) ListenForJobs(cb func(Work)) {
	go func() {
		for {
			w.WorkersChan <- w.JobsChan
			select {
			case job := <-w.JobsChan:
				cb(job)
			case <-w.End:
				zap.S().Info("[WorkQueue]- Stopped listening for jobs")
				return
			}
		}
	}()
}
