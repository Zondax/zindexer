package data_store

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"go.uber.org/zap"
)

type LocalClient struct {
	DataPath string
}

func newLocalClient(config DataStoreConfig) (*LocalClient, error) {
	return &LocalClient{DataPath: config.DataPath}, nil
}

func (c *LocalClient) GetFile(object string, bucket string) ([]byte, error) {
	targetObject := fmt.Sprintf("%s/%s/%s", c.DataPath, bucket, object)
	data, err := os.ReadFile(targetObject)
	if err != nil {
		zap.S().Errorf("err when getting object from store: %v", err.Error())
		return nil, err
	}

	return data, nil
}

func (c *LocalClient) List(bucket string, prefix string) ([]string, error) {
	var list []string
	files, err := os.ReadDir(fmt.Sprintf("%s/%s", c.DataPath, bucket))
	if err != nil {
		zap.S().Errorf("could not read directory '%s': %v", bucket, err)
		return nil, err
	}

	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, prefix) {
			list = append(list, fileName)
		}
	}

	return list, nil
}

func (c *LocalClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
	panic("not implemented")
}

func (c *LocalClient) UploadFromFile(name string, dest string) error {
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

	out, err := os.Create(fmt.Sprintf("%s/%s/%s", c.DataPath, dest, name))
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, file)
	return err
}

func (c *LocalClient) UploadFromBytes(data []byte, destFolder string, destName string) error {
	panic("not implemented")
}

func (c *LocalClient) StorageType() string {
	return LocalStorage
}
