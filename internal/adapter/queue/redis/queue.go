// Package redisqueue implements the queue, retry, DLQ, and scheduled-store ports
// on top of Redis.
package redisqueue

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

const queuePrefix = "queue:"

// order is the priority drain order: high first, then normal, then low.
var order = []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}

// Queue is a Redis-backed priority work queue using one list per priority.
type Queue struct{ rdb *redis.Client }

// New constructs a Queue.
func New(rdb *redis.Client) *Queue { return &Queue{rdb: rdb} }

func key(p domain.Priority) string { return queuePrefix + string(p) }

// Enqueue pushes an item onto its priority list.
func (q *Queue) Enqueue(ctx context.Context, item usecase.QueueItem) error {
	return q.rdb.LPush(ctx, key(item.Priority), item.NotificationID).Err()
}

// Dequeue blocks up to timeout for the highest-priority available item. It
// returns ok=false on timeout. Because BRPOP scans keys in priority order,
// higher-priority queues are always preferred while lower ones are still served
// whenever the higher queues are empty.
func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (usecase.QueueItem, bool, error) {
	keys := []string{key(domain.PriorityHigh), key(domain.PriorityNormal), key(domain.PriorityLow)}
	res, err := q.rdb.BRPop(ctx, timeout, keys...).Result()
	if errors.Is(err, redis.Nil) {
		return usecase.QueueItem{}, false, nil
	}
	if err != nil {
		return usecase.QueueItem{}, false, err
	}
	// res[0] is the key, res[1] is the value.
	p := domain.Priority(strings.TrimPrefix(res[0], queuePrefix))
	return usecase.QueueItem{NotificationID: res[1], Priority: p}, true, nil
}

// Remove deletes a notification id from all priority lists (used on cancel).
func (q *Queue) Remove(ctx context.Context, id string) error {
	for _, p := range order {
		if err := q.rdb.LRem(ctx, key(p), 0, id).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Depth returns the number of queued items at the given priority.
func (q *Queue) Depth(ctx context.Context, p domain.Priority) (int64, error) {
	return q.rdb.LLen(ctx, key(p)).Result()
}
