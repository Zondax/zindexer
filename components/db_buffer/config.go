package db_buffer

import "time"

type Config struct {
	SyncTimePeriod     time.Duration
	SyncBlockThreshold uint
}
