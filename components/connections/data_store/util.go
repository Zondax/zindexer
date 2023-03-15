package data_store

import (
    "log"
    "time"
)

func elapsed(start time.Time, message string) {
    elapsed := time.Since(start)
    log.Printf("%s duration %s\n", message, elapsed)
}
