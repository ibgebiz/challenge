// Package observability provides logging, metrics, and tracing setup.
package observability

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the Prometheus collectors for the notification system.
type Metrics struct {
	Enqueued    *prometheus.CounterVec
	Delivered   *prometheus.CounterVec
	Failed      *prometheus.CounterVec
	Retried     *prometheus.CounterVec
	RateLimited *prometheus.CounterVec
	Latency     *prometheus.HistogramVec
	QueueDepth  *prometheus.GaugeVec
	DLQSize     prometheus.Gauge
	InFlight    prometheus.Gauge
	reg         *prometheus.Registry
}

// NewMetrics constructs and registers all collectors on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg:         reg,
		Enqueued:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_enqueued_total", Help: "Notifications enqueued."}, []string{"channel", "priority"}),
		Delivered:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_delivered_total", Help: "Notifications delivered."}, []string{"channel"}),
		Failed:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_failed_total", Help: "Notifications failed (dead-lettered)."}, []string{"channel"}),
		Retried:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_retried_total", Help: "Notification retries scheduled."}, []string{"channel"}),
		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_rate_limited_total", Help: "Notifications re-queued due to rate limiting."}, []string{"channel"}),
		Latency:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notif_delivery_latency_seconds", Help: "Provider delivery latency.", Buckets: prometheus.DefBuckets}, []string{"channel"}),
		QueueDepth:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "notif_queue_depth", Help: "Current queue depth by priority."}, []string{"priority"}),
		DLQSize:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "notif_dlq_size", Help: "Current dead-letter queue size."}),
		InFlight:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "notif_in_flight", Help: "Notifications currently being delivered."}),
	}
	reg.MustRegister(m.Enqueued, m.Delivered, m.Failed, m.Retried, m.RateLimited,
		m.Latency, m.QueueDepth, m.DLQSize, m.InFlight)
	return m
}

// Registry returns the underlying Prometheus registry for exposition.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
