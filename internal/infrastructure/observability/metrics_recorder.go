package observability

import (
	"context"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// The methods below let *Metrics satisfy usecase.MetricsRecorder without the
// usecase layer depending on Prometheus.

// IncDelivered counts a delivered notification.
func (m *Metrics) IncDelivered(ch domain.Channel) { m.Delivered.WithLabelValues(string(ch)).Inc() }

// IncFailed counts a dead-lettered notification.
func (m *Metrics) IncFailed(ch domain.Channel) { m.Failed.WithLabelValues(string(ch)).Inc() }

// IncRetried counts a scheduled retry.
func (m *Metrics) IncRetried(ch domain.Channel) { m.Retried.WithLabelValues(string(ch)).Inc() }

// IncRateLimited counts a rate-limited (re-queued) notification.
func (m *Metrics) IncRateLimited(ch domain.Channel) { m.RateLimited.WithLabelValues(string(ch)).Inc() }

// ObserveLatency records provider delivery latency in seconds.
func (m *Metrics) ObserveLatency(ch domain.Channel, seconds float64) {
	m.Latency.WithLabelValues(string(ch)).Observe(seconds)
}

// DepthFunc reports the queue depth for a priority.
type DepthFunc func(ctx context.Context, p domain.Priority) (int64, error)

// SizeFunc reports the size of a store (e.g. the DLQ).
type SizeFunc func(ctx context.Context) (int64, error)

// RunGaugeUpdater periodically refreshes the queue-depth and DLQ-size gauges from
// shared Redis state until ctx is cancelled. These reflect system-wide state and
// are therefore exposed by the API process (which Prometheus scrapes).
func (m *Metrics) RunGaugeUpdater(ctx context.Context, interval time.Duration, depth DepthFunc, dlqSize SizeFunc) {
	t := time.NewTicker(interval)
	defer t.Stop()
	priorities := []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, p := range priorities {
				if n, err := depth(ctx, p); err == nil {
					m.QueueDepth.WithLabelValues(string(p)).Set(float64(n))
				}
			}
			if n, err := dlqSize(ctx); err == nil {
				m.DLQSize.Set(float64(n))
			}
		}
	}
}
