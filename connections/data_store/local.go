package data_store

import (
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

type LocalClient struct {
	DataPath string
}

func newLocalClient(config DataStoreConfig) (LocalClient, error) {
	return LocalClient{DataPath: config.DataPath}, nil
}

func (c LocalClient) GetFile(object string, bucket string) (*[]byte, error) {
	targetObject := fmt.Sprintf("%s/%s/%s", c.DataPath, bucket, object)
	data, err := ioutil.ReadFile(targetObject)
	if err != nil {
		zap.S().Errorf("err when getting object from store: %v", err.Error())
		return nil, err
	}

	return &data, nil
}

func (c LocalClient) List(bucket string, prefix string) *[]string {
	var list []string
	files, err := os.ReadDir(fmt.Sprintf("%s/%s", c.DataPath, bucket))
	if err != nil {
		zap.S().Errorf("could not read directory '%s': %v", bucket, err)
		return &list
	}

	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, prefix) {
			list = append(list, fileName)
		}
	}

	return &list
}

func (c LocalClient) UploadFile(name string, dest string) error {
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

func (c LocalClient) StorageType() string {
	return LocalStorage
}
