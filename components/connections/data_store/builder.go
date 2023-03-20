package data_store

import (
	"fmt"

	"go.uber.org/zap"
)

const (
	S5Storage    = "s5"
	S3Storage    = "s3"
	LocalStorage = "local"
	ContentType  = "application/octet-stream"
	DataPath     = "data"
	S3url        = "s3://"
)

func NewDataStoreClient(config DataStoreConfig) (DataStoreClient, error) {
	if len(config.ContentType) == 0 {
		config.ContentType = ContentType
	}
	if len(config.DataPath) == 0 {
		config.DataPath = DataPath
	}
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
