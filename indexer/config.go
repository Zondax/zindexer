package indexer

import (
	"github.com/Zondax/zindexer/components/connections/data_store"
	"github.com/Zondax/zindexer/components/db_buffer"
	"github.com/Zondax/zindexer/components/workQueue"
)

type ComponentsCfg struct {
	DBBufferCfg   db_buffer.Config
	DispatcherCfg WorkQueue.DispatcherConfig
	DataStoreCfg  data_store.DataStoreConfig
}

type Config struct {
	EnableBuffer bool
	ComponentsCfg
}
