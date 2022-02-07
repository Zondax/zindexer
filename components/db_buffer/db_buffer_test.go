package db_buffer

import (
	"fmt"
	"github.com/Zondax/zindexer"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const (
	TestSyncPeriod      = 5 * time.Second
	TestBlocksThreshold = 5
	TestTimeout         = TestSyncPeriod * 2
)

var (
	dbBuffer    *Buffer
	retrievedTx []ReportTransaction
)

type ReportTransaction struct {
	Height      int64  `json:"height" gorm:"index:idx_transactions_height"`
	TxTimestamp int64  `json:"tx_timestamp"`
	TxFrom      string `json:"tx_from" gorm:"index:idx_transactions_tx_from"`
}

func TestMain(m *testing.M) {
	zindexer.InitGlobalLogger()
	c := m.Run()
	os.Exit(c)
}

func createMockTx(height int) ReportTransaction {
	return ReportTransaction{
		Height:      int64(height),
		TxTimestamp: time.Now().Unix(),
	}
}

func SyncCallback() SyncResult {
	txs, err := dbBuffer.GetData("transaction")
	if err != nil {
		panic(err)
	}
	var syncedHeights []uint64
	for _, transactions := range txs {
		txs := transactions.([]ReportTransaction)
		for _, tx := range txs {
			syncedHeights = append(syncedHeights, uint64(tx.Height))
		}
		retrievedTx = append(retrievedTx, txs...)
	}
	fmt.Println("synced tx so far:", len(retrievedTx))
	return SyncResult{
		SyncedHeights: &syncedHeights,
		Error:         nil,
	}
}

func Test_InsertAndGetTransactions_BlocksThreshold(t *testing.T) {
	retrievedTx = nil
	dbBuffer = NewDBBuffer(nil, Config{
		SyncTimePeriod:     TestSyncPeriod,
		SyncBlockThreshold: TestBlocksThreshold,
	})

	dbBuffer.SetSyncFunc(SyncCallback)
	dbBuffer.Start()

	totalBlocks := TestBlocksThreshold
	totalTxsInBlock := 10

	var allTxs []ReportTransaction
	for h := 0; h < totalBlocks; h++ {
		var txs []ReportTransaction
		for t := 0; t < totalTxsInBlock; t++ {
			txs = append(txs, createMockTx(h))
		}

		allTxs = append(allTxs, txs...)
		err := dbBuffer.InsertData("transaction", int64(h), txs, true)
		fmt.Println("inserting mock tx for height", h)
		if err != nil {
			t.Fatal(err)
		}
	}

	go func() {
		time.Sleep(TestTimeout)
		panic("timeout when waiting for test to finish")
	}()

	// Wait until sync is completes
	<-dbBuffer.SyncComplete

	if eq := assert.ElementsMatch(t, allTxs, retrievedTx); !eq {
		t.Fatal("no match between transactions!")
	}

	dbBuffer.Stop()
}

func Test_InsertAndGetTransactions_Ticker(t *testing.T) {
	retrievedTx = nil
	dbBuffer = NewDBBuffer(nil, Config{
		SyncTimePeriod:     TestSyncPeriod,
		SyncBlockThreshold: TestBlocksThreshold,
	})

	dbBuffer.SetSyncFunc(SyncCallback)
	dbBuffer.Start()

	totalBlocks := TestBlocksThreshold - 1
	totalTxsInBlock := 10

	var allTxs []ReportTransaction
	for h := 0; h < totalBlocks; h++ {
		var txs []ReportTransaction
		for t := 0; t < totalTxsInBlock; t++ {
			txs = append(txs, createMockTx(h))
		}

		allTxs = append(allTxs, txs...)
		err := dbBuffer.InsertData("transaction", int64(h), txs, true)
		fmt.Println("inserting mock tx for height", h)
		if err != nil {
			t.Fatal(err)
		}
	}

	go func() {
		time.Sleep(TestTimeout)
		panic("timeout when waiting for test to finish")
	}()

	// Wait until sync is completes
	<-dbBuffer.SyncComplete

	if eq := assert.ElementsMatch(t, allTxs, retrievedTx); !eq {
		t.Fatal("no match between transactions!")
	}

	dbBuffer.Stop()
}
