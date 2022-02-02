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

func (j *IndexJobPool) GetNewJob() Work {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	if j.queue.Length() == 0 {
		return Work{JobId: -1}
	}
	return j.queue.Remove().(Work)
}

func (j *IndexJobPool) EnqueueJob(job Work) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.queue.Add(job)
}

func (j *IndexJobPool) EnqueueJobList(jobs *[]Work) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	for _, job := range *jobs {
		j.queue.Add(job)
	}
}
