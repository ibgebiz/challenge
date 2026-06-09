package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type stubQueue struct{ items chan usecase.QueueItem }

func (s *stubQueue) Enqueue(context.Context, usecase.QueueItem) error { return nil }

func (s *stubQueue) Dequeue(ctx context.Context, _ time.Duration) (usecase.QueueItem, bool, error) {
	select {
	case it := <-s.items:
		return it, true, nil
	case <-ctx.Done():
		return usecase.QueueItem{}, false, ctx.Err()
	}
}

func (s *stubQueue) Remove(context.Context, string) error { return nil }

func (s *stubQueue) Depth(context.Context, domain.Priority) (int64, error) { return 0, nil }

func TestLoop_ProcessesItem(t *testing.T) {
	q := &stubQueue{items: make(chan usecase.QueueItem, 1)}
	q.items <- usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}

	var processed int32
	proc := func(_ context.Context, _ usecase.QueueItem) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	Run(ctx, q, proc, 1)

	if atomic.LoadInt32(&processed) != 1 {
		t.Fatalf("want 1 processed, got %d", processed)
	}
}

func TestLoop_StopsOnCancel(t *testing.T) {
	q := &stubQueue{items: make(chan usecase.QueueItem)} // never produces
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	done := make(chan struct{})
	go func() {
		Run(ctx, q, func(context.Context, usecase.QueueItem) error { return nil }, 2)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}
