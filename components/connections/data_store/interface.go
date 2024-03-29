package data_store

import (
	"context"
	"fmt"
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
	DeleteFiles(name string, location string) error
	RenameFile(oldName string, newName string, bucket string) error
}

type DataStoreClient struct {
	Client IDataStoreClient
}

type DataStoreConfig struct {
	Url            string
	UseHttps       bool
	NoVerifySSL    bool
	User           string
	Password       string
	Service        string
	DataPath       string
	ContentType    string
	S3MaxRetries   int
	S3DelayRetries int
}

// Errors

var (
	ErrFileNotFound = fmt.Errorf("file not found")
)
