package data_store

import (
	"context"
	"io"
)

type IDataStoreClient interface {
	GetFile(name string, location string) ([]byte, error)
	UploadFromFile(filePath string, dest string) error
	UploadFromBytes(data []byte, destFolder string, destName string) error
	UploadFromReader(data io.Reader, size int64, destFolder string, destName string) error
	List(dir string, prefix string) ([]string, error)
	ListChan(ctx context.Context, dir string, prefix string) (<-chan string, error)
	StorageType() string
}

type DataStoreClient struct {
	Client IDataStoreClient
}

type DataStoreConfig struct {
	Url         string
	UseHttps    bool
	User        string
	Password    string
	Service     string
	DataPath    string
	ContentType string
}
