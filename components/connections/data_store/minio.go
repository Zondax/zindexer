package data_store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type MinioClient struct {
	client      *minio.Client
	contentType string
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

	return &MinioClient{
		client:      minioClient,
		contentType: config.ContentType,
	}, nil
}

func (c *MinioClient) GetClient() *minio.Client {
	return c.client
}

func (c *MinioClient) GetContentType() string {
	return c.contentType
}

func (c *MinioClient) GetFile(object string, bucket string) ([]byte, error) {
	if len(bucket) == 0 || len(object) == 0 {
		zap.S().Errorf("Bucket or object are empty")
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

func (c *MinioClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
	if len(bucket) == 0 || len(prefix) == 0 {
		zap.S().Errorf("Bucket or prefix are empty")
		return nil, fmt.Errorf("Bucket or prefix are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] List channel files")

	exists, err := c.GetClient().BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("bucket '%s' doesn't exists", bucket)
	}

	outChan := make(chan string, 10)
	go func() {
		defer close(outChan)

		for object := range c.GetClient().ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
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

func (c *MinioClient) UploadFromFile(name string, folder string) error {
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

	return c.UploadFromReader(file, fileStat.Size(), folder, file.Name())
}

func (c *MinioClient) UploadFromBytes(data []byte, folder string, name string) error {
	if len(data) == 0 || len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("Data, folder or name are empty")
		return fmt.Errorf("Data, folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from bytes")

	reader := bytes.NewReader(data)

	return c.UploadFromReader(reader, int64(reader.Len()), folder, name)
}

func (c *MinioClient) UploadFromReader(data io.Reader, size int64, folder string, name string) error {
	if len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("folder or name are empty")
		return fmt.Errorf("folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from reader")

	_, err := c.GetClient().PutObject(context.Background(), folder, name, data,
		size, minio.PutObjectOptions{ContentType: c.GetContentType()})
	if err != nil {
		return err
	}

	destUrl := fmt.Sprintf("%s%s/%s", S3url, folder, name)
	zap.S().Debugf("[%s] Operation: upload, Source: %s, Destination: %s, Size: %d", c.StorageType(), name, destUrl, size)

	return nil
}

func (c *MinioClient) StorageType() string {
	return S3Storage
}
