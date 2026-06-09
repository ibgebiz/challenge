package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type stubPromoter struct{ calls int32 }

func (s *stubPromoter) Tick(context.Context, time.Time) (int, error) {
	atomic.AddInt32(&s.calls, 1)
	return 0, nil
}

func TestScheduler_TicksUntilCancel(t *testing.T) {
	p := &stubPromoter{}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(120 * time.Millisecond); cancel() }()

	Run(ctx, p, 30*time.Millisecond)

	if atomic.LoadInt32(&p.calls) < 1 {
		t.Fatalf("expected at least 1 tick, got %d", p.calls)
	}
}
