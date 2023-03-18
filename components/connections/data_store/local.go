package data_store

import (
	"bytes"
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

func (c *LocalClient) GetFile(object string, bucket string) ([]byte, error) {
	if len(bucket) == 0 || len(object) == 0 {
		zap.S().Errorf("Bucket or object are empty")
		return nil, fmt.Errorf("Bucket or object are empty")
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
	if len(bucket) == 0 || len(prefix) == 0 {
		zap.S().Errorf("Bucket or prefix are empty")
		return nil, fmt.Errorf("Bucket or prefix are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] List files")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	list := []string{}
	reader, err := c.ListChan(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	for file := range reader {
		list = append(list, file)
	}

	return list, nil
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

func (c *LocalClient) UploadFromFile(name string, folder string) error {
	if len(name) == 0 || len(folder) == 0 {
		zap.S().Errorf("Name or folder are empty")
		return fmt.Errorf("Name or folder are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from file")

	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}

	if !fileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", name)
	}

	return c.UploadFromReader(file, fileStat.Size(), folder, fileStat.Name())
}

func (c *LocalClient) UploadFromBytes(data []byte, folder string, name string) error {
	if len(data) == 0 || len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("Data, folder or name are empty")
		return fmt.Errorf("Data, folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from bytes")

	reader := bytes.NewReader(data)

	return c.UploadFromReader(reader, int64(reader.Len()), folder, name)
}

func (c *LocalClient) UploadFromReader(data io.Reader, size int64, folder string, name string) error {
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
