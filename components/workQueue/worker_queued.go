package WorkQueue

import "go.uber.org/zap"

type IQueuedWorker interface {
	Start()
	DoWork(Job)
}

type QueuedWorker struct {
	Worker IQueuedWorker
}

type WorkerConstructor func(string, chan chan Job) QueuedWorker

type Job struct {
	JobId  int64
	Params interface{}
}

type WorkQueue struct {
	ID          string
	WorkersChan chan chan Job // used to communicate between dispatcher and workers
	JobsChan    chan Job
	End         chan bool
}

func (w WorkQueue) Stop() {
	w.End <- true
}

func (w WorkQueue) ListenForJobs(cb func(Job)) {
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
