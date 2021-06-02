package connections

import (
	"github.com/Zondax/zindexer"
	"gorm.io/gorm"
)

type GormConnection struct {
	db *gorm.DB
}

type DBQueryClient struct {
	Client zindexer.IDBQueryClient
}

type DBSubscriptionClient struct {
	Client zindexer.IDBSubscriptionClient
}

type TopicPubSubClient struct {
	Client zindexer.ITopicPubSubClient
}
