// Package worker runs the notification processing loop.
package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// ProcessFunc processes a single queue item. Implementations (e.g.
// usecase.ProcessNotification.Execute) handle retry/DLQ internally.
type ProcessFunc func(ctx context.Context, item usecase.QueueItem) error

// Run starts `concurrency` workers that dequeue and process items until ctx is
// cancelled. It blocks until all workers have stopped.
func Run(ctx context.Context, q usecase.Queue, process ProcessFunc, concurrency int) {
	if concurrency < 1 {
		concurrency = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, q, process)
		}()
	}
	wg.Wait()
}

func worker(ctx context.Context, q usecase.Queue, process ProcessFunc) {
	for {
		if ctx.Err() != nil {
			return
		}
		item, ok, err := q.Dequeue(ctx, time.Second)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			continue
		}
		if !ok {
			continue
		}
		// Errors (including ErrRateLimited) are handled inside process; ignore here.
		_ = process(ctx, item)
	}
}
