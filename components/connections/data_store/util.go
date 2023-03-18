package data_store

import (
	"time"

	"go.uber.org/zap"
)

func elapsed(start time.Time, message string) {
	elapsed := time.Since(start)
	zap.S().Debugf("%s duration %s", message, elapsed)
}
