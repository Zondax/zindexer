package indexer

import (
	"github.com/Zondax/zindexer/db_buffer"
	WorkQueue "github.com/Zondax/zindexer/workers/workQueue"
)

type ComponentsCfg struct {
	DBBufferCfg   db_buffer.Config
	DispatcherCfg WorkQueue.Config
}

type Config struct {
	ComponentsCfg
}
