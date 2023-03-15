package data_store

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"os"
	"time"
)

type MinioClient struct {
	client *minio.Client
}

func newMinioClient(config DataStoreConfig) (*MinioClient, error) {
	minioClient, err := minio.New(config.Url, &minio.Options{
		Creds:  credentials.NewStaticV4(config.User, config.Password, ""),
		Secure: config.UseHttps,
	})
	if err != nil {
		zap.S().Error(err.Error())
		return nil, err
	}

	return &MinioClient{client: minioClient}, nil
}

func (c *MinioClient) GetClient() *minio.Client {
	return c.client
}

func (c *MinioClient) GetFile(object string, bucket string) ([]byte, error) {
	if len(bucket) == 0 || len(object) == 0 {
		return nil, fmt.Errorf("Bucket or object are empty")
	}
	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Get file")

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

func (c *MinioClient) List(bucket string, prefix string) ([]string, error) {
	if len(bucket) == 0 || len(prefix) == 0 {
		return nil, fmt.Errorf("Bucket or prefix are empty")
	}
	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] List files")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var list []string

	reader, err := c.ListChan(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	for file := range reader {
		list = append(list, file)
	}

	return list, nil
}

func (c *MinioClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
	if len(bucket) == 0 || len(prefix) == 0 {
		return nil, fmt.Errorf("Bucket or prefix are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] List channel files")

	exists, err := c.client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("bucket '%s' doesn't exists", bucket)
	}

	outChan := make(chan string, 10)
	go func() {
		defer close(outChan)

		for object := range c.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			select {
			case <-ctx.Done():
				return
			default:
				outChan <- object.Key
			}
		}
	}()

	return outChan, nil
}

func (c *MinioClient) UploadFromFile(name string, dest string) error {
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

	_, err = c.client.PutObject(context.Background(), dest, file.Name(), file,
		fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return err
	}

	return nil
}

func (c *MinioClient) UploadFromBytes(data []byte, destFolder string, destName string) error {
	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from bytes")

	_, err := c.client.PutObject(context.Background(), destFolder, destName, bytes.NewReader(data),
		int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return err
	}
	return nil
}

func (c *MinioClient) StorageType() string {
	return S3Storage
}
