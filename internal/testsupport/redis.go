//go:build integration

// Package testsupport provides shared helpers for integration tests. It is only
// compiled under the "integration" build tag.
package testsupport

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisClient starts an ephemeral Redis container and returns a connected
// client. The container is terminated on test cleanup.
func RedisClient(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("redis endpoint: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: endpoint})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
