package database

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hasura/go-graphql-client"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
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
}

func NewGraphqlQueryClient(host string, token string) GraphqlClient {
	src := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	httpClient := oauth2.NewClient(context.Background(), src)
	return GraphqlClient{
		Host:   host,
		Client: graphql.NewClient(host, httpClient),
	}
}

func (c GraphqlClient) Connect() error {
	return nil
}

func NewGraphqlSubscriptionClient(host string, token string) (error, GraphqlSubscriptionClient) {
	client := graphql.NewSubscriptionClient(host).
		WithConnectionParams(map[string]interface{}{
			"headers": map[string]string{
				"x-hasura-admin-secret": token,
			},
		}).OnError(onClientError)

	return nil, GraphqlSubscriptionClient{Client: client}
}

func onClientError(sc *graphql.SubscriptionClient, err error) error {
	zap.S().Fatalf("Connection error on subscription client: %s", err.Error())
	return err
}

func (c GraphqlSubscriptionClient) Subscribe(query interface{}, handler func(message *json.RawMessage, err error) error) error {
	id, err := c.Client.Subscribe(query, nil, handler)
	if err != nil {
		return err
	}
	c.Id = id
	return nil
}

func (c GraphqlSubscriptionClient) Unsubscribe() error {
	err := c.Client.Unsubscribe(c.Id)
	if err != nil {
		return err
	}
	return nil
}

func (c GraphqlSubscriptionClient) Start() error {
	errCh := make(chan error)
	readyCh := make(chan bool)

	c.Client.OnConnected(func() {
		readyCh <- true
	})

	go func() {
		err := c.Client.Run()
		if err != nil {
			errCh <- err
		}
	}()

	for {
		select {
		case err := <-errCh:
			close(errCh)
			close(readyCh)
			return err
		case <-readyCh:
			close(readyCh)
			return nil
		case <-time.After(ConnectTimeout):
			return fmt.Errorf("timeout while waiting subscriber client to connect to host")
		}
	}
}

func (c GraphqlSubscriptionClient) Stop() error {
	err := c.Client.Close()
	if err != nil {
		return err
	}
	return nil
}
