package pub_sub

import (
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill/message"
)

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

type DBSubscriptionClient struct {
	Client IDBSubscriptionClient
}

type TopicPubSubClient struct {
	Client ITopicPubSubClient
}
