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

func genericList(c IDataStoreClient, bucket string, prefix string) ([]string, error) {
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

func putFromFile(c IDataStoreClient, srcPath string, dstPath string) error {
	// FIXME: this is unclear. We take the source name for the output name

	if len(srcPath) == 0 || len(dstPath) == 0 {
		zap.S().Errorf("srcPath or dstPath are empty")
		return fmt.Errorf("srcPath or dstPath are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from fileReader")

	fileReader, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer fileReader.Close()

	fileStat, err := fileReader.Stat()
	if err != nil {
		return err
	}

	if !fileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular fileReader", srcPath)
	}

	return c.PutFromReader(fileReader, fileStat.Size(), dstPath, fileStat.Name())
}

func putFromBytes(c IDataStoreClient, data []byte, dstPath string, dstName string) error {
	if len(data) == 0 || len(dstPath) == 0 || len(dstName) == 0 {
		zap.S().Errorf("Data, dstPath or dstName are empty")
		return fmt.Errorf("Data, dstPath or dstName are empty")
	}

	start := time.Now()
	defer elapsed(start, "["+c.StorageType()+"] Upload from bytes")

	reader := bytes.NewReader(data)

	return c.PutFromReader(reader, int64(reader.Len()), dstPath, dstName)
}

func testEndpoint(url string, insecureSkipVerify bool) error {
	transport := http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			// Modify the time to wait for a connection to establish
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: insecureSkipVerify},
		TLSHandshakeTimeout: 10 * time.Second,
	}

	client := http.Client{
		Transport: &transport,
		Timeout:   5 * time.Second,
	}
	_, err := client.Get(url)

	return err
}
