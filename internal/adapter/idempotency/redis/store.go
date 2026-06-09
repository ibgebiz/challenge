// Package redisidem implements the idempotency store on Redis.
package redisidem

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "idem:"

// Store deduplicates create requests by idempotency key.
type Store struct {
	rdb *redis.Client
	ttl time.Duration
}

// New constructs a Store whose keys expire after ttl.
func New(rdb *redis.Client, ttl time.Duration) *Store {
	return &Store{rdb: rdb, ttl: ttl}
}

// Remember sets key -> id only if the key is absent. If the key already exists
// it returns the previously stored id and found=true.
func (s *Store) Remember(ctx context.Context, key, id string) (string, bool, error) {
	ok, err := s.rdb.SetNX(ctx, keyPrefix+key, id, s.ttl).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return "", false, nil
	}
	existing, err := s.rdb.Get(ctx, keyPrefix+key).Result()
	if err != nil {
		return "", false, err
	}
	return existing, true, nil
}
