package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

type APIMetrics struct {
	Received          *prometheus.CounterVec
	Requests          *prometheus.HistogramVec
	StatusWriteErrors prometheus.Counter
}

func NewAPIMetrics(reg prometheus.Registerer) *APIMetrics {
	f := promauto.With(reg)
	return &APIMetrics{
		Received: f.NewCounterVec(prometheus.CounterOpts{
			Name: "events_received_total",
			Help: "Events received by the API, by result (accepted, invalid, unavailable, overloaded).",
		}, []string{"result"}),
		Requests: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route pattern and status code.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"route", "code"}),
		StatusWriteErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "status_write_errors_total",
			Help: "Best-effort pending status writes that failed (the event itself was accepted).",
		}),
	}
}

type ConsumerMetrics struct {
	Processed     *prometheus.CounterVec
	Failed        *prometheus.CounterVec
	Retried       *prometheus.CounterVec
	Duration      *prometheus.HistogramVec
	EndToEnd      prometheus.Histogram
	Lag           *prometheus.GaugeVec
	ActiveWorkers prometheus.Gauge
	CommitErrors  prometheus.Counter
}

func NewConsumerMetrics(reg prometheus.Registerer) *ConsumerMetrics {
	f := promauto.With(reg)
	return &ConsumerMetrics{
		Processed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Records handled successfully, by outcome (processed, duplicate).",
		}, []string{"outcome"}),
		Failed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Events dead-lettered, by reason (permanent, exhausted).",
		}, []string{"reason"}),
		Retried: f.NewCounterVec(prometheus.CounterOpts{
			Name: "events_retried_total",
			Help: "Processing retries, by error class (transient, unavailable).",
		}, []string{"class"}),
		Duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "Time to handle one record including retries, by outcome.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"outcome"}),
		EndToEnd: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_end_to_end_seconds",
			Help:    "Time from Kafka append to committed processing.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}),
		Lag: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Group lag per partition from the broker; every replica reports all partitions, so aggregate with max.",
		}, []string{"partition"}),
		ActiveWorkers: f.NewGauge(prometheus.GaugeOpts{
			Name: "active_workers",
			Help: "Partition workers currently processing records.",
		}),
		CommitErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "kafka_commit_errors_total",
			Help: "Failed offset commits (records will be redelivered).",
		}),
	}
}
