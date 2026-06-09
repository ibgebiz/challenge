//go:build integration

package redisratelimit

import (
	"context"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/testsupport"
)

func TestLimiter_BlocksOverCapacity(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	l := New(rdb, 2) // 2 per second
	ctx := context.Background()

	a1, _ := l.Allow(ctx, domain.ChannelSMS)
	a2, _ := l.Allow(ctx, domain.ChannelSMS)
	a3, _ := l.Allow(ctx, domain.ChannelSMS)
	if !a1 || !a2 {
		t.Fatal("first two should pass")
	}
	if a3 {
		t.Fatal("third should be blocked")
	}
}

func TestLimiter_PerChannelIndependent(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	l := New(rdb, 1)
	ctx := context.Background()

	sms, _ := l.Allow(ctx, domain.ChannelSMS)
	email, _ := l.Allow(ctx, domain.ChannelEmail)
	if !sms || !email {
		t.Fatal("different channels should not share a bucket")
	}
}
