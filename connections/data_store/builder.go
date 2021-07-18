package data_store

import (
	"fmt"
)

func NewDataStoreClient(config DataStoreConfig) (DataStoreClient, error) {
	switch config.Service {
	case MinIOStorage:
		client, err := newMinioClient(config)
		return DataStoreClient{client}, err
	default:
		return DataStoreClient{}, fmt.Errorf("unrecognized data store service '%s'", config.Service)
	}
}
