package data_store

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

type LocalClient struct {
	dataPath string
}

func newLocalClient(config DataStoreConfig) (*LocalClient, error) {
	return &LocalClient{
		dataPath: config.DataPath,
	}, nil
}

func (c *LocalClient) GetDataPath() string {
	return c.dataPath
}

func (c *LocalClient) Get(object string, bucket string) ([]byte, error) {
	if len(bucket) == 0 || len(object) == 0 {
		zap.S().Errorf("bucket or object are empty")
		return nil, fmt.Errorf("bucket or object are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Get file")

	targetObject := fmt.Sprintf("%s/%s/%s", c.GetDataPath(), bucket, object)
	data, err := os.ReadFile(targetObject)
	if err != nil {
		zap.S().Errorf("err when getting object from store: %v", err.Error())
		return nil, err
	}

	return data, nil
}

func (c *LocalClient) List(bucket string, prefix string) ([]string, error) {
	return genericList(c, bucket, prefix)
}

func (c *LocalClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
	if len(bucket) == 0 || len(prefix) == 0 {
		zap.S().Errorf("Bucket or prefix are empty")
		return nil, fmt.Errorf("Bucket or prefix are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] List channel files")

	list, err := filepath.Glob(fmt.Sprintf("%s/%s/%s*", c.GetDataPath(), bucket, prefix))
	if err != nil {
		zap.S().Errorf("could not read directory '%s': %v", bucket, err)
		return nil, err
	}

	outChan := make(chan string, 10)
	go func(files []string) {
		defer close(outChan)

		for _, f := range files {
			select {
			case <-ctx.Done():
				return
			default:
				outChan <- filepath.Base(f)
			}
		}
	}(list)

	return outChan, nil
}

func (c *LocalClient) PutFromFile(name string, folder string) error {
	return putFromFile(c, name, folder)
}

func (c *LocalClient) PutFromBytes(data []byte, folder string, name string) error {
	return putFromBytes(c, data, folder, name)
}

func (c *LocalClient) PutFromReader(data io.Reader, size int64, folder string, name string) error {
	if len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("Folder or name are empty")
		return fmt.Errorf("Folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from reader")

	destFolder := fmt.Sprintf("%s/%s", c.GetDataPath(), folder)
	if _, err := os.Stat(destFolder); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(destFolder, os.ModePerm); err != nil {
			return err
		}
	}
	destUrl := fmt.Sprintf("%s/%s", destFolder, name)
	out, err := os.Create(destUrl)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, data)

	zap.S().Debugf("[%s] Operation: upload, Source: %s, Destination: %s, Size: %d", c.StorageType(), name, destUrl, size)

	return err
}

func (c *LocalClient) StorageType() string {
	return LocalStorage
}
