package data_store

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
)

func elapsed(start time.Time, message string) {
	elapsed := time.Since(start)
	zap.S().Debugf("%s duration %s", message, elapsed)
}

func list(c IDataStoreClient, bucket string, prefix string) ([]string, error) {
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

func uploadFromFile(c IDataStoreClient, name string, folder string) error {
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

func uploadFromBytes(c IDataStoreClient, data []byte, folder string, name string) error {
	if len(data) == 0 || len(folder) == 0 || len(name) == 0 {
		zap.S().Errorf("Data, folder or name are empty")
		return fmt.Errorf("Data, folder or name are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from bytes")

	reader := bytes.NewReader(data)

	return c.UploadFromReader(reader, int64(reader.Len()), folder, name)
}

func testEndpoint(url string, skiptls bool) error {
	transport := http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			// Modify the time to wait for a connection to establish
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: skiptls},
		TLSHandshakeTimeout: 10 * time.Second,
	}

	client := http.Client{
		Transport: &transport,
		Timeout:   5 * time.Second,
	}
	_, err := client.Get(url)
	return err
}
