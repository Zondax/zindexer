package zindexer

import "gorm.io/gorm"

type DBConnection interface {
	GetDB() *gorm.DB
}

// IndexingWorker interface
type IndexingWorker interface {
	Index(from int64, to int64) error
}

// ChainIndexer
type ChainIndexer interface {
	MigrateTypes() error
	Start()
}
