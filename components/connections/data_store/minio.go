package data_store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
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
	useHTTPS := strings.HasPrefix(config.Url, "https://")

	u, err := url.Parse(config.Url)

	minioClient, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(config.User, config.Password, ""),
		Secure: useHTTPS,
	})

	if config.Tracing {
		minioClient.TraceOn(nil)
	}

	if err != nil {
		zap.S().Error(err.Error())
		return nil, err
	}

	return &MinioClient{
		client:      minioClient,
		contentType: config.ContentType,
	}, nil
}

func (c *MinioClient) GetContentType() string {
	return c.contentType
}

func (c *MinioClient) Get(objectName string, bucketName string) ([]byte, error) {
	if len(bucketName) == 0 || len(objectName) == 0 {
		zap.S().Errorf("Bucket or objectName are empty")
		return nil, fmt.Errorf("bucketName or objectName are empty")
	}
	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Get file")

	obj, err := c.client.GetObject(context.Background(), bucketName, objectName, minio.GetObjectOptions{})
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
	return genericList(c, bucket, prefix)
}

func (c *MinioClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
	if len(bucket) == 0 {
		return nil, fmt.Errorf("bucket is empty")
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

func (c *MinioClient) PutFromFile(srcPath string, dstPath string) error {
	return putFromFile(c, srcPath, dstPath)
}

func (c *MinioClient) PutFromBytes(data []byte, dstPath string, dstName string) error {
	return putFromBytes(c, data, dstPath, dstName)
}

func (c *MinioClient) PutFromReader(data io.Reader, size int64, folder string, name string) error {
	if len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("folder or name are empty")
		return fmt.Errorf("folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from reader")

	_, err := c.client.PutObject(
		context.Background(),
		folder, name, data, size,
		minio.PutObjectOptions{ContentType: c.GetContentType()})

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
