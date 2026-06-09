package observability

import "testing"

func TestMetrics_Register(t *testing.T) {
	m := NewMetrics()
	// Exercising the collectors must not panic or duplicate-register.
	m.Enqueued.WithLabelValues("sms", "high").Inc()
	m.Delivered.WithLabelValues("sms").Inc()
	m.Failed.WithLabelValues("sms").Inc()
	m.QueueDepth.WithLabelValues("high").Set(3)
	m.DLQSize.Set(1)
	if m.Registry() == nil {
		t.Fatal("registry should not be nil")
	}
}

func TestNewMetrics_Independent(t *testing.T) {
	// Each call uses its own registry, so constructing twice must not panic.
	a := NewMetrics()
	b := NewMetrics()
	if a.Registry() == b.Registry() {
		t.Fatal("each Metrics should own a distinct registry")
	}
}
