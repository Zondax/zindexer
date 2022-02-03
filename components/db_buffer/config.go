package db_buffer

import "time"

const DefaultSyncPeriod = 30 * time.Second

type Config struct {
	SyncTimePeriod     time.Duration
	SyncBlockThreshold uint
}
