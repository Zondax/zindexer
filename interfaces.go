package zindexer

import (
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
	"gorm.io/gorm"
)

type DBConnection interface {
	GetDB() *gorm.DB
}

type IDBQueryClient interface {
	Connect() error
}

type IDBSubscriptionClient interface {
	Subscribe(query interface{}, handler func(message *json.RawMessage, err error) error) error
	Unsubscribe() error
	Start() error
	Stop() error
}

type ITopicPubSubClient interface {
	Subscribe(string, func(messages <-chan *message.Message)) error
	Publish(string, *message.Message) error
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
