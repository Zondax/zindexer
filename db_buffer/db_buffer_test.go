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

func createMockTx(height int) *ReportTransaction {
	return &ReportTransaction{
		Height:      int64(height),
		TxTimestamp: time.Now().Unix(),
	}
}

func Test_InsertAndGetTransactions_BlocksThreshold(t *testing.T) {
	var retrievedTx []*ReportTransaction
	dbBuffer := NewDBBuffer(nil, Config{
		SyncTimePeriod:     TestSyncPeriod,
		SyncBlockThreshold: TestBlocksThreshold,
	})

	dbBuffer.SetSyncFunc(
		func() SyncResult {
			txs, err := dbBuffer.GetData("transaction")
			if err != nil {
				t.Fatal(err)
			}
			var syncedHeights []uint64
			for _, transactions := range txs {
				txs := *transactions.(*[]*ReportTransaction)
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
		})

	dbBuffer.Start()

	maxBlocks := TestBlocksThreshold * 200
	maxTxsInBlock := 10

	var allTxs []*ReportTransaction
	for h := 0; h < maxBlocks; h++ {
		var txs []*ReportTransaction
		for t := 0; t < maxTxsInBlock; t++ {
			txs = append(txs, createMockTx(h))
		}

		allTxs = append(allTxs, txs...)
		err := dbBuffer.InsertData("transaction", int64(h), &txs, true)
		fmt.Println("inserting mock tx for height", h)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Give some time to sync func to be executed
	time.Sleep(10 * time.Second)

	if eq := assert.ElementsMatch(t, allTxs, retrievedTx); !eq {
		t.Fatal("no match between transactions!")
	}

	dbBuffer.Stop()
}

func Test_InsertAndGetTransactions_Ticker(t *testing.T) {
	dbBuffer := NewDBBuffer(nil, Config{
		SyncTimePeriod:     TestSyncPeriod,
		SyncBlockThreshold: TestBlocksThreshold,
	})
	dbBuffer.Start()

	maxBlocks := 1
	maxTxsInBlock := 10

	for h := 0; h < maxBlocks; h++ {
		var txs []*ReportTransaction
		for t := 0; t < maxTxsInBlock; t++ {
			txs = append(txs, createMockTx(h))
		}

		if h > 0 && dbBuffer.GetBufferSize("transaction") == 0 {
			t.Fatal("Unexpected empty buffer!")
		}

		err := dbBuffer.InsertData("transaction", int64(h), &txs, false)

		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(TestSyncPeriod + (5 * time.Second))
	if dbBuffer.GetBufferSize("transaction") > 0 {
		t.Fatal("Sync by ticker failed!")
	}

	dbBuffer.Stop()
}
