package WorkQueue

import (
	"github.com/eapache/queue"
	"sync"
)

type IndexJobPool struct {
	mutex sync.Mutex
	queue *queue.Queue
}

type PoolConfig struct {
	StartHeight int64
	EndHeight   int64
}

func NewJobPool() *IndexJobPool {
	pool := &IndexJobPool{
		mutex: sync.Mutex{},
		queue: queue.New(),
	}

	return pool
}

func (j *IndexJobPool) GetNewJob() Job {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	if j.queue.Length() == 0 {
		return Job{JobId: -1}
	}
	return j.queue.Remove().(Job)
}

func (j *IndexJobPool) EnqueueJob(job Job) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.queue.Add(job)
}

func (j *IndexJobPool) EnqueueJobList(jobs *[]Job) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	for _, job := range *jobs {
		j.queue.Add(job)
	}
}
