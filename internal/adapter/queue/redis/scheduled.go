package redisqueue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

const scheduledKey = "scheduled:zset"

// Scheduled holds future-dated notifications until they become due.
type Scheduled struct{ z zsetStore }

// NewScheduled constructs a Scheduled store.
func NewScheduled(rdb *redis.Client) *Scheduled {
	return &Scheduled{z: zsetStore{rdb: rdb, key: scheduledKey}}
}

// Add stores an item to be promoted at time at.
func (s *Scheduled) Add(ctx context.Context, item usecase.QueueItem, at time.Time) error {
	return s.z.add(ctx, item, at)
}

// Remove deletes a scheduled item by notification id (used on cancel).
func (s *Scheduled) Remove(ctx context.Context, id string) error {
	return s.z.removeByID(ctx, id)
}

// DuePromote moves all due items into dst.
func (s *Scheduled) DuePromote(ctx context.Context, now time.Time, dst usecase.Queue) (int, error) {
	return s.z.duePromote(ctx, now, dst)
}
