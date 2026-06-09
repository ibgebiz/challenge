// Package redisclient constructs the shared Redis client.
package redisclient

import "github.com/redis/go-redis/v9"

// New returns a Redis client for the given address.
func New(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}
