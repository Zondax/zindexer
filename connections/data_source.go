package connections

import (
	"context"
	ds "github.com/Zondax/zindexer/connections/data_store"
	db "github.com/Zondax/zindexer/connections/database"
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
	DataBase      db.DBQueryClient
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
		client, err := ds.NewDataStoreClient(cfg)
		if err != nil {
			panic(err)
		}
		w.DataStore = client
	}
}
