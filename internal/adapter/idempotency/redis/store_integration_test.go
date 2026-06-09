//go:build integration

package redisidem

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/testsupport"
)

func TestStore_RememberFirstWins(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	s := New(rdb, time.Minute)
	ctx := context.Background()

	existing, found, err := s.Remember(ctx, "k1", "id-1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("first call should not find an existing entry")
	}
	if existing != "" {
		t.Fatalf("want empty existing, got %q", existing)
	}

	existing, found, err = s.Remember(ctx, "k1", "id-2")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("second call should find the existing entry")
	}
	if existing != "id-1" {
		t.Fatalf("want id-1, got %q", existing)
	}
}
