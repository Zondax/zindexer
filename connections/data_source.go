package connections

import (
	"context"
	ds "github.com/Zondax/zindexer/connections/data_store"
	"github.com/coinbase/rosetta-sdk-go/client"
	"go.mongodb.org/mongo-driver/mongo"
	"gorm.io/gorm"
	"time"
)

const (
	defaultRetryDelay = 30 * time.Second
)

type DataSource struct {
	// data sources
	DatabasePostgres *gorm.DB
	DatabaseMongo    *mongo.Client
	RosettaClient    *client.APIClient
	NodeClient       interface{}
	DataStore        ds.DataStoreClient
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

func WithPostgresDB(dbConn *gorm.DB) SourceOption {
	checkPointer(dbConn)
	return func(w *DataSource) {
		w.DatabasePostgres = dbConn
	}
}

func WithMongoDB(dbConn *mongo.Client) SourceOption {
	checkPointer(dbConn)
	return func(w *DataSource) {
		w.DatabaseMongo = dbConn
	}
}

func WithRosettaClient(client *client.APIClient) SourceOption {
	checkPointer(client)
	return func(w *DataSource) {
		w.RosettaClient = client
	}
}

func WithNodeClient(node interface{}) SourceOption {
	checkPointer(node)
	return func(w *DataSource) {
		w.NodeClient = node
	}
}

func WithDataStore(cfg ds.DataStoreConfig) SourceOption {
	return func(w *DataSource) {
		storeClient, err := ds.NewDataStoreClient(cfg)
		if err != nil {
			panic(err)
		}
		w.DataStore = storeClient
	}
}

func checkPointer(p interface{}) {
	if p == nil {
		panic("Pointer cannot be null!")
	}
}
