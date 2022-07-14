package db_buffer

import (
	"fmt"
	"github.com/Zondax/zindexer/components/connections/zmetrics"
	"github.com/Zondax/zindexer/components/tracker"
	cmap "github.com/orcaman/concurrent-map"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"strconv"
	"sync"
	"time"
)

var (
	defaultBucketTime = []float64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60}
)

type SyncCB func() SyncResult

type SyncResult struct {
	Id            string
	SyncedHeights *[]uint64 // Synced heights
	Error         error     // Error in db insertion process
}

type BufferMetrics struct {
	totalSyncTimeHist zmetrics.Histogram
}

type Buffer struct {
	buffer       map[string]cmap.ConcurrentMap
	dbConn       *gorm.DB
	syncMutex    sync.Mutex
	syncTicker   *time.Ticker
	newDataChan  chan string
	exitChan     chan bool
	metrics      BufferMetrics
	config       Config
	syncCb       SyncCB
	enabled      bool
	SyncComplete chan SyncResult
}

func NewDBBuffer(db *gorm.DB, cfg Config) *Buffer {
	b := &Buffer{
		buffer:      make(map[string]cmap.ConcurrentMap),
		dbConn:      db,
		syncTicker:  time.NewTicker(cfg.SyncTimePeriod),
		newDataChan: make(chan string),
		exitChan:    make(chan bool, 1),
		config:      cfg,
		syncCb: func() SyncResult {
			return SyncResult{
				"",
				nil,
				fmt.Errorf("no sync function defined. Call SetSyncFunc"),
			}
		},
		SyncComplete: make(chan SyncResult, 1),
	}

	b.registerMetrics()
	b.syncTicker.Stop()
	return b
}

// Start starts listening for syncing triggering events
func (b *Buffer) Start() {
	go b.checkIsTimeToSync()
	b.enabled = true
}

// Stop stops listening for syncing triggering events
func (b *Buffer) Stop() {
	b.enabled = false
	b.syncTicker.Stop()
	// closes the loop in func checkIsTimeToSync
	b.exitChan <- true

	// wait if a syncing event is in progress,
	// syncMutex gets released in func callSync
	b.syncMutex.Lock()
	defer b.syncMutex.Unlock()
}

// SetSyncFunc sets the syncing callback function
func (b *Buffer) SetSyncFunc(cb SyncCB) {
	b.syncCb = cb
}

// InsertData inserts 'data' into the buffer under the key 'key'
// if notify is set to true, the condition 'SyncBlockThreshold' will be tested for that specific key
func (b *Buffer) InsertData(key string, height int64, data interface{}, notify bool) error {
	if !b.enabled {
		return nil
	}

	b.syncMutex.Lock()
	defer b.syncMutex.Unlock()

	if _, ok := b.buffer[key]; !ok {
		zap.S().Debugf("[Buffer] created new map for key %s", key)
		b.buffer[key] = cmap.New()
	}

	b.buffer[key].Set(strconv.FormatInt(height, 10), data)

	if notify {
		// this is done to write to the newDataChan in a non-blocking way
		select {
		case b.newDataChan <- key:
		default:
		}
	}

	b.syncTicker.Reset(b.config.SyncTimePeriod)
	return nil
}

func (b *Buffer) ClearBuffer(dataType string) {
	if m, ok := b.buffer[dataType]; ok {
		m.Clear()
	}
}

func (b *Buffer) clearAllBuffers() {
	for _, m := range b.buffer {
		m.Clear()
	}
}

func (b *Buffer) GetBufferSize(dataType string) int {
	size := 0
	if m, ok := b.buffer[dataType]; ok {
		size = m.Count()
	}
	return size
}

func (b *Buffer) GetData(dataType string) (map[string]interface{}, error) {
	if m, ok := b.buffer[dataType]; ok {
		return m.Items(), nil
	} else {
		return nil, fmt.Errorf("[Buffer] buffer doesn't contain dataType %s", dataType)
	}
}

func (b *Buffer) callSync() {
	zap.S().Debugf("[Buffer] callSync started ...")
	defer func() {
		zap.S().Debugf("[Buffer] callSync finished!")
	}()

	b.syncTicker.Stop()
	b.syncMutex.Lock()
	defer b.syncMutex.Unlock()
	defer b.syncTicker.Reset(b.config.SyncTimePeriod)

	syncStart := time.Now()
	syncResult := b.syncCb()

	b.clearAllBuffers()

	if syncResult.Error == nil && syncResult.SyncedHeights != nil {
		timeTotal := time.Since(syncStart).Seconds()
		if timeTotal > 0 {
			zap.S().Debugf("[Buffer] Total DB insertion time took %v seconds", timeTotal)
			b.metrics.totalSyncTimeHist.Observe(timeTotal)
		}
	}

	b.onDBSyncComplete(&syncResult)

	select {
	case b.SyncComplete <- syncResult:
	default:
	}
}

func (b *Buffer) checkIsTimeToSync() {
	for {
		select {
		case <-b.syncTicker.C:
			zap.S().Debug("[Buffer] Syncing because of Ticker...")
			b.callSync()
		case key := <-b.newDataChan:
			l := uint(b.GetBufferSize(key))
			if l >= b.config.SyncBlockThreshold {
				zap.S().Debugf("[Buffer] Syncing because of blocks amount: %d", l)
				b.callSync()
			}
		case <-b.exitChan:
			zap.S().Debug("[Buffer] Exiting...")
			return
		}
	}
}

func (b *Buffer) registerMetrics() {
	b.metrics.totalSyncTimeHist = zmetrics.NewHistogram(zmetrics.HistogramOpts{
		Namespace: "blocks",
		Subsystem: "buffer",
		Name:      "sync_total_time_seconds",
		Help:      "Total time spent by function 'syncCb'",
		Buckets:   defaultBucketTime,
	})

	err := zmetrics.RegisterMetric(b.metrics.totalSyncTimeHist)
	if err != nil {
		zap.S().Error("Could not register Metric: totalSyncTimeHist")
	}
}

func (b *Buffer) onDBSyncComplete(r *SyncResult) {
	if r.SyncedHeights == nil {
		zap.S().Errorf("onDBSyncComplete received nil SyncedHeights. Check db_sync code!")
		return
	}

	if r.Id == "" {
		zap.S().Errorf("onDBSyncComplete received an empty 'Id'. Check db_sync code!")
		return
	}

	if r.Error != nil {
		zap.S().Errorf(r.Error.Error())
		// Remove WIP heights
		_ = tracker.UpdateInProgressHeight(false, r.SyncedHeights, r.Id, b.dbConn)
		return
	}

	err := tracker.UpdateAndRemoveWipHeights(r.SyncedHeights, r.Id, b.dbConn)
	if err != nil {
		return
	}
}
