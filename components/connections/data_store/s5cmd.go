package data_store

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	s5store "github.com/peak/s5cmd/storage"
	s5url "github.com/peak/s5cmd/storage/url"
	"go.uber.org/zap"
)

const (
	s5UploadConcurrency  = 5
	s5UploadPartSize     = 10 * 1024 * 1024 // MiB
	s5UploadStorageClass = "STANDARD"
)

type S5cmdClient struct {
	client      *s5store.S3
	contentType string
	access_key  string
	secret_key  string
}

type S5cmdList struct {
	Key string `json:"key"`
}

func newS5cmdClient(config DataStoreConfig) (*S5cmdClient, error) {
	storeOpts := s5store.Options{
		MaxRetries:  5,
		Endpoint:    "https://" + config.Url,
		NoVerifySSL: config.UseHttps,
		DryRun:      false,
	}
	storeUrl := &s5url.URL{Type: 0}
	os.Setenv("AWS_ACCESS_KEY", config.User)
	os.Setenv("AWS_SECRET_KEY", config.Password)
	defer os.Unsetenv("AWS_ACCESS_KEY")
	defer os.Unsetenv("AWS_SECRET_KEY")
	client, err := s5store.NewRemoteClient(context.Background(), storeUrl, storeOpts)
	if err != nil {
		zap.S().Error(err.Error())
		return nil, err
	}

	return &S5cmdClient{
		client:      client,
		contentType: config.ContentType,
		access_key:  config.User,
		secret_key:  config.Password,
	}, nil
}

func (c *S5cmdClient) GetClient() *s5store.S3 {
	return c.client
}

func (c *S5cmdClient) GetContentType() string {
	return c.contentType
}

func (c *S5cmdClient) GetFile(object string, bucket string) ([]byte, error) {
	if len(bucket) == 0 || len(object) == 0 {
		zap.S().Errorf("Bucket or object are empty")
		return nil, fmt.Errorf("Bucket or object are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Get file")

	storeUrl, err := s5url.New(S3url + bucket + "/" + object)
	if err != nil {
		return nil, err
	}
	rc, err := c.GetClient().Read(context.Background(), storeUrl)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (c *S5cmdClient) List(bucket string, prefix string) ([]string, error) {
	return list(c, bucket, prefix)
}

func (c *S5cmdClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
	if len(bucket) == 0 || len(prefix) == 0 {
		zap.S().Errorf("Bucket or prefix are empty")
		return nil, fmt.Errorf("Bucket or prefix are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] List channel files")

	storeUrl, err := s5url.New(S3url + bucket + "/" + prefix)
	if err != nil {
		return nil, err
	}

	outChan := make(chan string, 10)
	go func() {
		defer close(outChan)

		reader := c.GetClient().List(ctx, storeUrl, false)
		for {
			select {
			case object := <-reader:
				if object == nil || object.Err != nil {
					return
				}
				outChan <- strings.TrimPrefix(object.URL.String(), S3url+bucket+"/")
			case <-ctx.Done():
				return
			}
		}
	}()

	return outChan, nil
}

func (c *S5cmdClient) UploadFromFile(name string, folder string) error {
	return uploadFromFile(c, name, folder)
}

func (c *S5cmdClient) UploadFromBytes(data []byte, folder string, name string) error {
	return uploadFromBytes(c, data, folder, name)
}

func (c *S5cmdClient) UploadFromReader(data io.Reader, size int64, folder string, name string) error {
	if len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("folder or name are empty")
		return fmt.Errorf("folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from reader")

	destUrl, err := s5url.New(S3url + folder + "/" + name)
	if err != nil {
		return err
	}

	metadata := s5store.NewMetadata().
		SetStorageClass(string(s5UploadStorageClass)).
		SetContentType(c.GetContentType())

	ctx := context.Background()
	err = c.GetClient().Put(ctx, data, destUrl, metadata, s5UploadConcurrency, s5UploadPartSize)
	if err != nil {
		return err
	}

	zap.S().Debugf("[%s] Operation: upload, Source: %s, Destination: %s, Size: %d", c.StorageType(), name, destUrl, size)

	return nil
}

func (c *S5cmdClient) StorageType() string {
	return S5Storage
}
