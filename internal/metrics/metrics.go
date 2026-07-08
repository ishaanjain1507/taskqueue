package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_jobs_processed_total",
			Help: "Total number of jobs processed, partitioned by status and type",
		},
		[]string{"status", "type"},
	)

	JobProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "taskqueue_job_processing_duration_seconds",
			Help:    "Histogram of job processing durations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type"},
	)

	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "taskqueue_http_requests_total",
			Help: "Total number of HTTP requests, partitioned by method and endpoint",
		},
		[]string{"method", "endpoint", "status"},
	)

	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "taskqueue_http_request_duration_seconds",
			Help:    "Histogram of HTTP request durations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)
)
