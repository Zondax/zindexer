package data_store

import (
	"fmt"

	"go.uber.org/zap"
)

const (
	S5Storage    = "s5"
	S3Storage    = "s3"
	LocalStorage = "local"
)

func NewDataStoreClient(config DataStoreConfig) (DataStoreClient, error) {
	zap.S().Infof("[DataStore] - Creating client for service '%s'", config.Service)
	switch config.Service {
	case S5Storage:
		client, err := newS5cmdClient(config)
		return DataStoreClient{client}, err
	case S3Storage:
		client, err := newMinioClient(config)
		return DataStoreClient{client}, err
	case LocalStorage:
		client, err := newLocalClient(config)
		return DataStoreClient{client}, err
	default:
		return DataStoreClient{}, fmt.Errorf("unrecognized data store service '%s'", config.Service)
	}
}
