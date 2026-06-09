package redisqueue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

const retryKey = "retry:zset"

// Retry holds items awaiting a future retry attempt.
type Retry struct{ z zsetStore }

// NewRetry constructs a Retry queue.
func NewRetry(rdb *redis.Client) *Retry { return &Retry{z: zsetStore{rdb: rdb, key: retryKey}} }

// Schedule enqueues an item to be retried at time at.
func (r *Retry) Schedule(ctx context.Context, item usecase.QueueItem, at time.Time) error {
	return r.z.add(ctx, item, at)
}

// DuePromote moves all due items into dst.
func (r *Retry) DuePromote(ctx context.Context, now time.Time, dst usecase.Queue) (int, error) {
	return r.z.duePromote(ctx, now, dst)
}

// Size returns the number of items awaiting retry.
func (r *Retry) Size(ctx context.Context) (int64, error) { return r.z.size(ctx) }
