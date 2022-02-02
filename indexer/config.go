package indexer

import (
	"github.com/Zondax/zindexer/components/db_buffer"
	"github.com/Zondax/zindexer/components/workQueue"
)

type ComponentsCfg struct {
	DBBufferCfg   db_buffer.Config
	DispatcherCfg WorkQueue.DispatcherConfig
}

type Config struct {
	ComponentsCfg
}
