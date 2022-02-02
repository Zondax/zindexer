package db_buffer

import (
	"bytes"
	"fmt"
	"github.com/jszwec/csvutil"
	"github.com/spf13/viper"
	"github.com/zondax/zindexer-filecoin/types"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"
)

func InsertTxsToDB(transactions interface{}, tableName string) error {
	if transactions == nil {
		return nil
	}

	filePath, err := writeCsvRecords(tableName, transactions)
	if err != nil {
		return err
	}

	defer func(filePath string) {
		err := deleteFile(filePath)
		if err != nil {
			zap.S().Error(err)
		}
	}(filePath)

	err = uploadCSV(filePath, tableName)
	if err != nil {
		return err
	}

	return nil
}

func writeCsvRecords(itemName string, records interface{}) (string, error) {
	marshaled, err := csvutil.Marshal(records)
	if err != nil {
		zap.S().Error(err.Error())
		return "", err
	}

	currTime := time.Now().Unix()
	filePath := fmt.Sprintf("%s/%s_%d.csv", ".", itemName, currTime)
	f, err := os.Create(filePath)

	if err != nil {
		zap.S().Errorf("failed to open file, err: %v", err)
		return "", err
	}
	defer f.Close()

	file, err := os.OpenFile(
		filePath,
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		zap.S().Error(err)
		return "", err
	}
	defer file.Close()

	_, err2 := file.Write(marshaled)
	if err2 != nil {
		zap.S().Error(err2)
		return "", err2
	}
	return filePath, nil
}

func uploadCSV(filePath string, destinationTable string) error {
	cmdPath := viper.GetString("db_batch_insert_cmd")

	var (
		dbName     = viper.GetString("db.name")
		dbHost     = viper.GetString("db.host")
		dbPort     = viper.GetString("db.port")
		dbUser     = viper.GetString("db.user")
		dbPassword = viper.GetString("db.password")
		dbSchema   = viper.GetString("db_schema")
	)

	workersCount := strconv.FormatInt(int64(runtime.NumCPU()), 10)
	tableName := types.GetTableNameWithoutSchema(destinationTable)
	dbConnParams := fmt.Sprintf("'host=%s user=%s password=%s port=%s sslmode=disable'", dbHost, dbUser, dbPassword, dbPort)
	insertParams := fmt.Sprintf("%s --db-name %s --schema %s --table %s --connection %s --file %s --workers %s --skip-header", cmdPath, dbName, dbSchema, tableName, dbConnParams, filePath, workersCount)
	cmd := exec.Command("/bin/sh", "-c", insertParams)

	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		zap.S().Errorf("Error executing CLI command to upload CSV file %s: %+v (%+v) (%+v)", filePath, stderr.String(), out.String(), err.Error())
		return err
	}

	return nil
}

func deleteFile(filePath string) error {
	err := os.Remove(filePath)
	if err != nil {
		zap.S().Errorf("Error removing file %s after DB upload: %s", filePath, err.Error())
		return err
	}

	return nil
}
