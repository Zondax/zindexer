package WorkQueue

import "time"

const DefaultRetryTimeout = 30 * time.Second

type DispatcherConfig struct {
	RetryTimeout time.Duration
}
