package graphql

import (
	"encoding/json"
	"fmt"
	"github.com/Zondax/zindexer/connections/database"
	"github.com/hasura/go-graphql-client"
	"go.uber.org/zap"
	"net/http"
	"time"
)

const ConnectTimeout = 10 * time.Second

type GraphqlClient struct {
	Host   string
	Client *graphql.Client
}

type GraphqlSubscriptionClient struct {
	Client *graphql.SubscriptionClient
	Id     string
	// state
	errChan   chan error
	readyChan chan bool
	connected bool
}

func NewGraphqlQueryClient(host string, token string) *GraphqlClient {
	transport := http.DefaultTransport
	if token != "" {
		transport = database.AuthHeaderTransport{Transport: http.DefaultTransport, Token: token}
	}

	customHttpClient := http.Client{Transport: transport}
	return &GraphqlClient{
		Host:   host,
		Client: graphql.NewClient(host, &customHttpClient),
	}
}

func (c *GraphqlClient) Connect() error {
	return nil
}

func NewGraphqlSubscriptionClient(host string, token string) (error, *GraphqlSubscriptionClient) {
	client := graphql.NewSubscriptionClient(host).
		WithConnectionParams(map[string]interface{}{
			"headers": map[string]string{
				"x-hasura-admin-secret": token,
			},
		}).OnError(onClientError)

	subClient := &GraphqlSubscriptionClient{
		Client:    client,
		errChan:   make(chan error),
		readyChan: make(chan bool),
		connected: false,
	}

	client.OnConnected(subClient.onClientConnected)
	client.OnDisconnected(subClient.onClientDisconnected)

	return nil, subClient
}

func (c *GraphqlSubscriptionClient) onClientConnected() {
	zap.S().Infof("Graphql client connected")
	c.connected = true
	c.readyChan <- true
}

func (c *GraphqlSubscriptionClient) onClientDisconnected() {
	zap.S().Warnf("Graphql client disconnected")
	c.connected = false
	c.readyChan <- false
}

func onClientError(sc *graphql.SubscriptionClient, err error) error {
	zap.S().Fatalf("Connection error on subscription client: %s", err.Error())
	return err
}

func (c *GraphqlSubscriptionClient) Subscribe(query interface{}, handler func(message *json.RawMessage, err error) error) error {
	id, err := c.Client.Subscribe(query, nil, handler)
	if err != nil {
		return err
	}
	c.Id = id
	return nil
}

func (c *GraphqlSubscriptionClient) Unsubscribe() error {
	err := c.Client.Unsubscribe(c.Id)
	if err != nil {
		return err
	}
	return nil
}

func (c *GraphqlSubscriptionClient) Start() error {
	go func() {
		err := c.Client.Run()
		if err != nil {
			c.errChan <- err
		}
	}()

	for {
		select {
		case err := <-c.errChan:
			return err
		case <-c.readyChan:
			return nil
		case <-time.After(ConnectTimeout):
			return fmt.Errorf("timeout while waiting subscriber client to connect to host")
		}
	}
}

func (c *GraphqlSubscriptionClient) Stop() error {
	return c.Client.Close()
}

func (c *GraphqlSubscriptionClient) GetState() bool {
	return c.connected
}
