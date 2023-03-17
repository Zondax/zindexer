package data_store

import (
	"log"
	"path/filepath"
	"time"
)

func elapsed(start time.Time, message string) {
	elapsed := time.Since(start)
	log.Printf("%s duration %s\n", message, elapsed)
}

func filename(file string) string {
	return filepath.Base(file)
}
