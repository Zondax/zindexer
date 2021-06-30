package pub_sub

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var ChannelPubSubDefaultConfig = gochannel.Config{
	OutputChannelBuffer:            10,
	Persistent:                     true,
	BlockPublishUntilSubscriberAck: false,
}

type ChannelPubSub struct {
	Client *gochannel.GoChannel
}

// NewPubSubHandlerChannel this handler uses Watermill's go channels
// implementation as message engine
func NewPubSubHandlerChannel(config gochannel.Config) ChannelPubSub {
	if config == (gochannel.Config{}) {
		// Use default config
		config = ChannelPubSubDefaultConfig
	}

	ps := gochannel.NewGoChannel(
		config,
		watermill.NewStdLogger(false, false),
	)

	return ChannelPubSub{
		Client: ps,
	}
}

func (p ChannelPubSub) Subscribe(topic string, cb func(messages <-chan *message.Message)) error {
	messages, err := p.Client.Subscribe(context.Background(), topic)
	if err != nil {
		fmt.Printf("Could not subscribe to topic %s", topic)
		return err
	}

	go cb(messages)
	return nil
}

func (p ChannelPubSub) Publish(topic string, msg *message.Message) error {
	err := p.Client.Publish(topic, msg)
	if err != nil {
		fmt.Printf("Could not publish to topic %s", topic)
		return err
	}

	return nil
}
