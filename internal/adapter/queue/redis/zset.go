package redisqueue

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// member encodes a queue item as "priority|notificationID".
func encode(item usecase.QueueItem) string {
	return string(item.Priority) + "|" + item.NotificationID
}

func decode(m string) usecase.QueueItem {
	parts := strings.SplitN(m, "|", 2)
	if len(parts) != 2 {
		return usecase.QueueItem{NotificationID: m, Priority: domain.PriorityNormal}
	}
	return usecase.QueueItem{Priority: domain.Priority(parts[0]), NotificationID: parts[1]}
}

// zsetStore is a Redis sorted set keyed by due-time used for delayed promotion
// of queue items (retries and scheduled notifications).
type zsetStore struct {
	rdb *redis.Client
	key string
}

func (z *zsetStore) add(ctx context.Context, item usecase.QueueItem, at time.Time) error {
	return z.rdb.ZAdd(ctx, z.key, redis.Z{Score: float64(at.Unix()), Member: encode(item)}).Err()
}

// duePromote moves every item whose score is <= now into dst, removing each from
// the set first so concurrent promoters cannot double-enqueue it.
func (z *zsetStore) duePromote(ctx context.Context, now time.Time, dst usecase.Queue) (int, error) {
	members, err := z.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     z.key,
		ByScore: true,
		Start:   "-inf",
		Stop:    strconv.FormatInt(now.Unix(), 10),
	}).Result()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, m := range members {
		removed, err := z.rdb.ZRem(ctx, z.key, m).Result()
		if err != nil {
			return count, err
		}
		if removed == 0 {
			continue // another promoter already claimed it
		}
		if err := dst.Enqueue(ctx, decode(m)); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (z *zsetStore) size(ctx context.Context) (int64, error) {
	return z.rdb.ZCard(ctx, z.key).Result()
}

// removeByID removes any member whose encoded value references the given id.
func (z *zsetStore) removeByID(ctx context.Context, id string) error {
	members, err := z.rdb.ZRange(ctx, z.key, 0, -1).Result()
	if err != nil {
		return err
	}
	for _, m := range members {
		if decode(m).NotificationID == id {
			if err := z.rdb.ZRem(ctx, z.key, m).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}
