package data_store

// FIXME: rename to dataStore

import (
	"context"
	"io"
)

type IDataStoreClient interface {
	StorageType() string

	Get(objectName string, bucket string) ([]byte, error)

	PutFromFile(filePath string, dest string) error
	PutFromBytes(data []byte, destFolder string, destName string) error
	PutFromReader(data io.Reader, size int64, destFolder string, destName string) error

	List(bucket string, prefix string) ([]string, error)
	ListChan(ctx context.Context, dir string, prefix string) (<-chan string, error)
}

// FIXME: make this an enum? or use it as a prefix?
const (
	S5Storage    = "s5"
	S3Storage    = "s3"
	LocalStorage = "local"
)

type DataStoreConfig struct {
	// can be s3, s5, local
	Service string

	// FIXME: it should follow https://en.wikipedia.org/wiki/Uniform_Resource_Identifier#Syntax
	Url                string
	InsecureSkipVerify bool

	User     string
	Password string

	DataPath    string
	ContentType string

	Tracing bool
}
