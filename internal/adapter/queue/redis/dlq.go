package redisqueue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const dlqKey = "dlq:list"

// DLQ is the dead-letter queue for notifications that exhausted their retries.
type DLQ struct{ rdb *redis.Client }

// NewDLQ constructs a DLQ.
func NewDLQ(rdb *redis.Client) *DLQ { return &DLQ{rdb: rdb} }

// Push records a dead-lettered notification id and the reason it failed.
func (d *DLQ) Push(ctx context.Context, id, reason string) error {
	return d.rdb.LPush(ctx, dlqKey, id+"|"+reason).Err()
}

// Size returns the number of dead-lettered items.
func (d *DLQ) Size(ctx context.Context) (int64, error) {
	return d.rdb.LLen(ctx, dlqKey).Result()
}
