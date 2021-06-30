package connections

import (
	"context"
	"fmt"
	ds "github.com/Zondax/zindexer/connections/data_store"
	"github.com/coinbase/rosetta-sdk-go/client"
	"gorm.io/gorm"
	"time"
)

const defaultRetryDelay = time.Duration(30 * time.Second)

type DataTransport struct {
	// data sources
	DbConn        *gorm.DB
	RosettaClient *client.APIClient
	NodeClient    interface{}
	DataStore     ds.DataStoreClient
	// common
	Ctx        context.Context
	RetryDelay time.Duration
}

type TransportOption func(*DataTransport)

func NewDataTransport(opts ...TransportOption) DataTransport {
	d := DataTransport{
		Ctx:        context.Background(),
		RetryDelay: defaultRetryDelay,
	}
	for _, opt := range opts {
		opt(&d)
	}

	return d
}

func WithContext(ctx context.Context) TransportOption {
	return func(w *DataTransport) {
		w.Ctx = ctx
	}
}

func WithRetryDelay(delay time.Duration) TransportOption {
	return func(w *DataTransport) {
		w.RetryDelay = delay
	}
}

func WithDBConnection(dbConn *gorm.DB) TransportOption {
	return func(w *DataTransport) {
		w.DbConn = dbConn
	}
}

func WithRosettaClient(client *client.APIClient) TransportOption {
	return func(w *DataTransport) {
		w.RosettaClient = client
	}
}

func WithNodeClient(node interface{}) TransportOption {
	return func(w *DataTransport) {
		w.NodeClient = node
	}
}

func WithDataStore(cfg ds.DataStoreConfig) TransportOption {
	return func(w *DataTransport) {
		switch cfg.Service {
		case "minio":
			c, _ := ds.NewMinioClient(cfg)
			w.DataStore = ds.DataStoreClient{Client: c}
			return
		default:
			panic(fmt.Errorf("DataStore with service %s, is not available", cfg.Service))
		}
	}
}
