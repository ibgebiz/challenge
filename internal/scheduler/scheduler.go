// Package scheduler periodically promotes due items into the work queue.
package scheduler

import (
	"context"
	"time"
)

// Promoter performs one promotion pass for the current time.
type Promoter interface {
	Tick(ctx context.Context, now time.Time) (int, error)
}

// Run invokes the promoter on every interval tick until ctx is cancelled.
func Run(ctx context.Context, p Promoter, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			_, _ = p.Tick(ctx, now)
		}
	}
}
