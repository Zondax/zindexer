package WorkQueue

import "time"

type DispatcherConfig struct {
	RetryTimeout time.Duration
}
