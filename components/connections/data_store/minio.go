package data_store

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"os"
)

type MinioClient struct {
	client *minio.Client
}

func newMinioClient(config DataStoreConfig) (MinioClient, error) {
	minioClient, err := minio.New(config.Url, &minio.Options{
		Creds:  credentials.NewStaticV4(config.User, config.Password, ""),
		Secure: config.UseHttps,
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

func (c MinioClient) GetFile(object string, bucket string) ([]byte, error) {
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
	return data, nil
}

func (c MinioClient) List(bucket string, prefix string) ([]string, error) {
	var list []string
	exists, err := c.client.BucketExists(context.Background(), bucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("bucket '%s' doesn't exists", bucket)
	}

	for object := range c.client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		list = append(list, object.Key)
	}

	return list, nil
}

func (c MinioClient) UploadFromFile(name string, dest string) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}

	_, err = c.client.PutObject(context.Background(), dest, file.Name(), file,
		fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return err
	}

	return nil
}

func (c MinioClient) UploadFromBytes(data []byte, destFolder string, destName string) error {
	_, err := c.client.PutObject(context.Background(), destFolder, destName, bytes.NewReader(data),
		int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return err
	}
	return nil
}

func (c MinioClient) StorageType() string {
	return S3Storage
}
