package scheduler

import (
	"context"
	"time"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// DuePromoter is implemented by both usecase.ScheduledStore and usecase.RetryQueue:
// it moves all items due at `now` into the destination queue.
type DuePromoter interface {
	DuePromote(ctx context.Context, now time.Time, dst usecase.Queue) (int, error)
}

// QueuePromoter adapts a DuePromoter into a Promoter targeting a fixed queue.
type QueuePromoter struct {
	Src DuePromoter
	Dst usecase.Queue
}

// Tick promotes all due items from the source into the destination queue.
func (p QueuePromoter) Tick(ctx context.Context, now time.Time) (int, error) {
	return p.Src.DuePromote(ctx, now, p.Dst)
}
