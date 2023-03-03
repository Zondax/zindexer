package zmetrics

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type PrometheusCollector prometheus.Collector
type CounterOpts prometheus.CounterOpts
type GaugeOpts prometheus.GaugeOpts
type HistogramOpts prometheus.HistogramOpts

type Counter prometheus.Counter
type CounterVec *prometheus.CounterVec
type Gauge prometheus.Gauge
type Histogram prometheus.Histogram

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

func prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := newResponseWriter(w)
		next.ServeHTTP(rw, r)
	})
}

// StartMetricsServer starts a prometheus server.
// Data Url is at localhost:<port>/metrics/<endpoint>
func StartMetricsServer(port string, endpoint string) chan error {
	router := mux.NewRouter()
	router.Use(prometheusMiddleware)

	// Prometheus endpoint
	router.Path("/metrics/" + endpoint).Handler(promhttp.Handler())
	errChan := make(chan error)

	go func() {
		server := &http.Server{
			Addr:              fmt.Sprintf(":%s", port),
			ReadHeaderTimeout: 5 * time.Second,
			Handler:           router,
		}

		err := server.ListenAndServe()
		if err != nil {
			zap.S().Errorf("Prometheus server error: %v", err)
			errChan <- err
		} else {
			zap.S().Infof("Prometheus server serving at port %s", port)
		}
	}()

	return errChan
}

func RegisterMetric(c PrometheusCollector) error {
	return prometheus.Register(c)
}

func NewSimpleCounter(opts CounterOpts) Counter {
	return prometheus.NewCounter(prometheus.CounterOpts(opts))
}

func NewVecCounter(opts CounterOpts, labels []string) CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts(opts), labels)
}

func NewGauge(opts GaugeOpts) Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts(opts))
}

func NewHistogram(opts HistogramOpts) prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts(opts))
}
