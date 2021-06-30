package data_store

import (
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type MinioClient struct {
	client *minio.Client
}

func NewMinioClient(config DataStoreConfig) (MinioClient, error) {
	minioClient, err := minio.New(config.Url, &minio.Options{
		Creds:  credentials.NewStaticV4(config.User, config.Password, ""),
		Secure: true,
	})
	if err != nil {
		zap.S().Error(err.Error())
		return MinioClient{}, err
	}

	return MinioClient{client: minioClient}, nil
}

func (c MinioClient) GetClient() *minio.Client {
	return c.client
}

func (c MinioClient) GetFile(object string, bucket string) (*[]byte, error) {
	obj, err := c.GetClient().GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj)
	if err != nil {
		return nil, err
	}

	data := buf.Bytes()
	return &data, nil
}

func (c MinioClient) List(bucket string, prefix string) *[]string {
	var list []string
	for object := range c.client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		list = append(list, object.Key)
	}

	return &list
}

func (c MinioClient) UploadFile(name string, dest string) error {
	// TODO
	return nil
}
