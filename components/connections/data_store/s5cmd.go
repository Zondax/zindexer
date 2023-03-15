package data_store

import (
    "context"
    "fmt"
    s5store "github.com/peak/s5cmd/storage"
    s5url "github.com/peak/s5cmd/storage/url"
    "io"
    "os"
    "strings"
    "time"
)

type S5cmdClient struct {
    client     *s5store.S3
    access_key string
    secret_key string
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
        return nil, err
    }

    return &S5cmdClient{
        client:     client,
        access_key: config.User,
        secret_key: config.Password,
    }, nil
}

func (c *S5cmdClient) GetFile(object string, bucket string) ([]byte, error) {
    if len(bucket) == 0 || len(object) == 0 {
        return nil, fmt.Errorf("Bucket or object are empty")
    }

    start := time.Now()
    defer elapsed(start, "["+c.StorageType()+"] Get file")

    storeUrl, err := s5url.New("s3://" + bucket + "/" + object)
    if err != nil {
        return nil, err
    }
    rc, err := c.client.Read(context.Background(), storeUrl)
    if err != nil {
        return nil, err
    }
    defer rc.Close()
    return io.ReadAll(rc)
}

func (c *S5cmdClient) List(bucket string, prefix string) ([]string, error) {
    if len(bucket) == 0 || len(prefix) == 0 {
        return nil, fmt.Errorf("Bucket or prefix are empty")
    }

    start := time.Now()
    defer elapsed(start, "["+c.StorageType()+"] List files")

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    out := []string{}
    reader, err := c.ListChan(ctx, bucket, prefix)
    if err != nil {
        return nil, err
    }
    for file := range reader {
        out = append(out, file)
    }
    return out, nil
}

func (c *S5cmdClient) ListChan(ctx context.Context, bucket string, prefix string) (<-chan string, error) {
    if len(bucket) == 0 || len(prefix) == 0 {
        return nil, fmt.Errorf("Bucket or prefix are empty")
    }

    start := time.Now()
    defer elapsed(start, "["+c.StorageType()+"] List channel files")

    storeUrl, err := s5url.New("s3://" + bucket + "/" + prefix)
    if err != nil {
        return nil, err
    }

    outChan := make(chan string, 10)
    go func() {
        defer close(outChan)

        reader := c.client.List(ctx, storeUrl, false)
        for {
            select {
            case object := <-reader:
                if object == nil || object.Err != nil {
                    return
                }
                outChan <- strings.TrimPrefix(object.URL.String(), "s3://"+bucket+"/")
            case <-ctx.Done():
                return
            }
        }
    }()

    return outChan, nil
}

func (c *S5cmdClient) UploadFromFile(name string, dest string) error {
    start := time.Now()
    defer elapsed(start, "["+c.StorageType()+"] Upload from file")
    return nil
}

func (c *S5cmdClient) UploadFromBytes(data []byte, destFolder string, destName string) error {
    start := time.Now()
    defer elapsed(start, "["+c.StorageType()+"] Upload from bytes")
    return nil
}

func (c *S5cmdClient) StorageType() string {
    return S5Storage
}
