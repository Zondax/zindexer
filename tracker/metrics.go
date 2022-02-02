package tracker

import (
	"fmt"
	"github.com/Zondax/zindexer/connections/zmetrics"
	"go.uber.org/zap"
	"regexp"
	"strings"
)

var (
	metricsMap    = make(map[string]*zmetrics.Gauge)
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")
)

func setTotalMissingHeightsMetric(id string, count int) {
	ind := getOrCreateIndicator(id)
	if ind == nil {
		zap.S().Warnf("Could not update metrics dor id '%s', nil indicator found!", id)
		return
	}

	(*ind).Set(float64(count))
}

func updateMissingHeights(id string, delta int) {
	ind := getOrCreateIndicator(id)
	if ind == nil {
		zap.S().Warnf("Could not update metrics dor id '%s', nil indicator found!", id)
		return
	}

	(*ind).Sub(float64(delta))
}

func createNewIndicator(id string) (error, *zmetrics.Gauge) {
	m := zmetrics.NewGauge(zmetrics.GaugeOpts{
		Namespace: "zindexer",
		Subsystem: "tracker",
		Name:      formatIdForName(id),
		Help:      fmt.Sprintf("Remaning blocks for indexer %s", id),
	})

	m.Set(0)
	err := zmetrics.RegisterMetric(m)
	if err != nil {
		zap.S().Errorf("Could not register Metric for id: %s", id)
		return err, nil
	}

	metricsMap[id] = &m
	return nil, &m
}

func getOrCreateIndicator(id string) *zmetrics.Gauge {
	if ind, ok := metricsMap[id]; ok {
		return ind
	}

	err, ind := createNewIndicator(id)
	if err != nil {
		zap.S().Warnf("Could not create indicator: %v", err)
		return nil
	}

	return ind
}

func formatIdForName(name string) string {
	s := matchFirstCap.ReplaceAllString(name, "${1}_${2}")
	s = matchAllCap.ReplaceAllString(s, "${1}_${2}")
	s = strings.ToLower(s)
	return fmt.Sprintf("remaning_blocks_%s", s)
}
