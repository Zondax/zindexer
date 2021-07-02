package connections

import (
	"context"
	"fmt"
	ds "github.com/Zondax/zindexer/connections/data_store"
	"github.com/coinbase/rosetta-sdk-go/client"
	"gorm.io/gorm"
	"time"
)

const (
	defaultRetryDelay = 30 * time.Second
)

type DataSource struct {
	// data sources
	DbConn        *gorm.DB
	RosettaClient *client.APIClient
	NodeClient    interface{}
	DataStore     ds.DataStoreClient
	// common
	Ctx        context.Context
	RetryDelay time.Duration
}

type SourceOption func(*DataSource)

func NewDataSource(opts ...SourceOption) DataSource {
	d := DataSource{
		Ctx:        context.Background(),
		RetryDelay: defaultRetryDelay,
	}
	for _, opt := range opts {
		opt(&d)
	}

	return d
}

func WithContext(ctx context.Context) SourceOption {
	return func(w *DataSource) {
		w.Ctx = ctx
	}
}

func WithRetryDelay(delay time.Duration) SourceOption {
	return func(w *DataSource) {
		w.RetryDelay = delay
	}
}

func WithDBConnection(dbConn *gorm.DB) SourceOption {
	return func(w *DataSource) {
		w.DbConn = dbConn
	}
}

func WithRosettaClient(client *client.APIClient) SourceOption {
	return func(w *DataSource) {
		w.RosettaClient = client
	}
}

func WithNodeClient(node interface{}) SourceOption {
	return func(w *DataSource) {
		w.NodeClient = node
	}
}

func WithDataStore(cfg ds.DataStoreConfig) SourceOption {
	return func(w *DataSource) {
		switch cfg.Service {
		case ds.MinIOStorage:
			c, _ := ds.NewMinioClient(cfg)
			w.DataStore = ds.DataStoreClient{Client: c}
			return
		default:
			panic(fmt.Errorf("DataStore with service %s, is not available", cfg.Service))
		}
	}
}
