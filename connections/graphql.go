package connections

import (
	"context"
	"encoding/json"
	"github.com/hasura/go-graphql-client"
	"golang.org/x/oauth2"
)

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
	// TODO: Test connection with a dummy query
	return nil
}

func NewGraphqlSubscriptionClient(host string, token string) (error, GraphqlSubscriptionClient) {
	client := graphql.NewSubscriptionClient(host).
		WithConnectionParams(map[string]interface{}{
			"headers": map[string]string{
				"x-hasura-admin-secret": token,
			},
		})

	return nil, GraphqlSubscriptionClient{Client: client}
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
	err := c.Client.Run()
	if err != nil {
		return err
	}
	return nil
}

func (c GraphqlSubscriptionClient) Stop() error {
	err := c.Client.Close()
	if err != nil {
		return err
	}
	return nil
}
