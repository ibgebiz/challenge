package ws

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

const eventsChannel = "notif:events"

// RedisPublisher implements usecase.EventPublisher by publishing status events to
// a Redis pub/sub channel. Worker processes use this so the API process (which
// holds the WebSocket connections) can fan events out to clients.
type RedisPublisher struct{ rdb *redis.Client }

// NewRedisPublisher constructs a RedisPublisher.
func NewRedisPublisher(rdb *redis.Client) *RedisPublisher { return &RedisPublisher{rdb: rdb} }

// Publish marshals the event and publishes it; errors are best-effort.
func (p *RedisPublisher) Publish(ctx context.Context, e usecase.StatusEvent) {
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	_ = p.rdb.Publish(ctx, eventsChannel, b).Err()
}

// BridgeToHub subscribes to the Redis events channel and forwards every event to
// the local Hub. It blocks until ctx is cancelled.
func BridgeToHub(ctx context.Context, rdb *redis.Client, hub *Hub, logger *slog.Logger) {
	sub := rdb.Subscribe(ctx, eventsChannel)
	defer func() { _ = sub.Close() }()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var e usecase.StatusEvent
			if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
				if logger != nil {
					logger.Warn("failed to decode status event", "error", err)
				}
				continue
			}
			hub.Publish(ctx, e)
		}
	}
}

// Compile-time check that RedisPublisher satisfies the port.
var _ usecase.EventPublisher = (*RedisPublisher)(nil)
