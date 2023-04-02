package data_store

import (
	"fmt"
	"go.uber.org/zap"
)

const (
	ContentType = "application/octet-stream"
	DataPath    = "data"
	S3url       = "s3://"
)

func NewDataStoreClient(config DataStoreConfig) (IDataStoreClient, error) {
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
		return client, err
	case S3Storage:
		client, err := newMinioClient(config)
		return client, err
	case LocalStorage:
		client, err := newLocalClient(config)
		return client, err
	default:
		return nil, fmt.Errorf("unrecognized data store service '%s'", config.Service)
	}
}
