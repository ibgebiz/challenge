// Package redisratelimit implements a per-channel rate limiter on Redis.
package redisratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// Limiter enforces a per-channel maximum messages-per-second using a fixed
// one-second window counter.
type Limiter struct {
	rdb    *redis.Client
	perSec int
}

// New constructs a Limiter allowing perSec messages per channel per second.
func New(rdb *redis.Client, perSec int) *Limiter {
	return &Limiter{rdb: rdb, perSec: perSec}
}

// Allow reports whether a token was available for the channel in the current
// one-second window.
func (l *Limiter) Allow(ctx context.Context, ch domain.Channel) (bool, error) {
	window := time.Now().Unix()
	k := fmt.Sprintf("rate:%s:%d", ch, window)
	n, err := l.rdb.Incr(ctx, k).Result()
	if err != nil {
		return false, err
	}
	if n == 1 {
		// Expire shortly after the window closes; ignore the (rare) expire error.
		_ = l.rdb.Expire(ctx, k, 2*time.Second).Err()
	}
	return n <= int64(l.perSec), nil
}
