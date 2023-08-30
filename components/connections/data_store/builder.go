package data_store

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	S5Storage           = "s5"
	S3Storage           = "s3"
	LocalStorage        = "local"
	ContentType         = "application/octet-stream"
	DataPath            = "data"
	S3url               = "s3://"
	Localurl            = "file://"
	defaultMaxRetries   = 3
	defaultDelayRetries = 5
)

func NewDataStoreClient(config DataStoreConfig) (DataStoreClient, error) {
	if !isValidService(config.Service) {
		return DataStoreClient{}, fmt.Errorf("unrecognized data store service '%s'", config.Service)
	}

	setDefaultTypes(&config)

	zap.S().Infof("[DataStore] - Creating client for service '%s'", config.Service)

	var client DataStoreClient
	var err error

	for i := 0; i < config.S3MaxRetries; i++ {
		client, err = getDataStoreConnection(config)

		if err == nil {
			return client, nil
		}

		time.Sleep(time.Second * time.Duration(config.S3DelayRetries))
	}

	return DataStoreClient{}, err
}

func getDataStoreConnection(config DataStoreConfig) (DataStoreClient, error) {
	switch config.Service {
	case S5Storage:
		s5Client, err := newS5cmdClient(config)
		return DataStoreClient{Client: s5Client}, err
	case S3Storage:
		minioClient, err := newMinioClient(config)
		return DataStoreClient{Client: minioClient}, err
	case LocalStorage:
		localClient, err := newLocalClient(config)
		return DataStoreClient{Client: localClient}, err
	default:
		return DataStoreClient{}, errors.New("invalid service type")
	}
}

func isValidService(service string) bool {
	return service == S5Storage || service == S3Storage || service == LocalStorage
}

func setDefaultTypes(config *DataStoreConfig) {
	if len(config.ContentType) == 0 {
		config.ContentType = ContentType
	}
	if len(config.DataPath) == 0 {
		config.DataPath = DataPath
	}
	if config.S3MaxRetries == 0 {
		config.S3MaxRetries = defaultMaxRetries
	}
	if config.S3DelayRetries == 0 {
		config.S3DelayRetries = defaultDelayRetries
	}
}
