package graphql

import (
	"encoding/json"
	"fmt"
	"github.com/Zondax/zindexer/components/connections/database"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/hasura/go-graphql-client"
	"go.uber.org/zap"
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
	if c.connected {
		// Client already connected, reset the client just in case
		zap.S().Warnf("Client already connected, resetting...")
		err := c.Client.Reset()
		if err != nil {
			zap.S().Warnf("Could not reset Graphql client")
		}
		return
	}

	c.connected = true
	c.readyChan <- true
}

func (c *GraphqlSubscriptionClient) onClientDisconnected() {
	zap.S().Warnf("Graphql client disconnected")
	c.connected = false
	c.readyChan <- false
}

func onClientError(sc *graphql.SubscriptionClient, err error) error {
	zap.S().Errorf("Connection error on subscription client: %s", err.Error())
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

// Hasura Specific Helpers

func HasuraApiRequest(host string, token string, body string) error {
	payload := strings.NewReader(body)
	client := &http.Client{}
	req, err := http.NewRequest("POST", host, payload)

	if err != nil {
		return err
	}
	req.Header.Add("x-hasura-admin-secret", token)
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	b, err := ioutil.ReadAll(res.Body)
	zap.S().Debug(string(b))
	if err != nil {
		return err
	}
	return nil
}

func HasuraCreateView(host string, token string, viewName string, viewAs string) error {
	host = strings.Replace(host, "graphql", "query", 1)
	body := fmt.Sprintf(`
	{
		"type": "run_sql",
		"args": {
			"sql": "CREATE VIEW %s as %s"
		}
	}`, viewName, viewAs)

	err := HasuraApiRequest(host, token, body)
	if err != nil {
		return err
	}
	return nil
}

func HasuraTrackTable(host string, token string, table string) error {
	schemaAndTable := strings.SplitN(table, ".", 2)
	host = strings.Replace(host, "graphql", "query", 1)
	body := fmt.Sprintf(`
	{
		"type": "track_table",
		"args": {
			"schema": "%s",
			"name": "%s"
		}
	}`, schemaAndTable[0], strings.ToLower(schemaAndTable[1]))

	err := HasuraApiRequest(host, token, body)
	if err != nil {
		return err
	}
	return nil

}
