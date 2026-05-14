package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collectors holds all Prometheus metrics exposed by the platform.
type Collectors struct {
	IngestedTotal    prometheus.Counter
	RejectedTotal    prometheus.Counter
	WriteLatency     prometheus.Histogram
	WriteErrors      prometheus.Counter
	QueueDepth       prometheus.Gauge
	BatchSize        prometheus.Histogram
	AlertsFiredTotal prometheus.Counter
	RequestsTotal    *prometheus.CounterVec
	RequestLatency   *prometheus.HistogramVec
}

// New constructs and registers all collectors. Safe to call once at startup.
func New() *Collectors {
	return &Collectors{
		IngestedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "logstream_ingested_total",
			Help: "Total log events accepted for ingestion.",
		}),
		RejectedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "logstream_rejected_total",
			Help: "Total log events rejected by validation.",
		}),
		WriteLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "logstream_mongo_write_latency_seconds",
			Help:    "Latency of MongoDB bulk write operations.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
		}),
		WriteErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "logstream_mongo_write_errors_total",
			Help: "Total MongoDB write errors.",
		}),
		QueueDepth: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "logstream_queue_depth",
			Help: "Current depth of the ingestion buffer.",
		}),
		BatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "logstream_batch_size",
			Help:    "Distribution of bulk write batch sizes.",
			Buckets: []float64{1, 10, 25, 50, 100, 200, 500},
		}),
		AlertsFiredTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "logstream_alerts_fired_total",
			Help: "Total alerts dispatched.",
		}),
		RequestsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "logstream_http_requests_total",
			Help: "Total HTTP requests by endpoint and status.",
		}, []string{"method", "path", "status"}),
		RequestLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "logstream_http_request_latency_seconds",
			Help:    "HTTP request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
	}
}
