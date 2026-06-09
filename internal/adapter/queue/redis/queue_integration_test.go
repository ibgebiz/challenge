//go:build integration

package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/testsupport"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func TestQueue_PriorityOrder(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	q := New(rdb)
	ctx := context.Background()

	_ = q.Enqueue(ctx, usecase.QueueItem{NotificationID: "low", Priority: domain.PriorityLow})
	_ = q.Enqueue(ctx, usecase.QueueItem{NotificationID: "high", Priority: domain.PriorityHigh})

	item, ok, err := q.Dequeue(ctx, time.Second)
	if err != nil || !ok {
		t.Fatalf("dequeue: %v ok=%v", err, ok)
	}
	if item.NotificationID != "high" {
		t.Fatalf("want high first, got %s", item.NotificationID)
	}
}

func TestQueue_DequeueTimeout(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	q := New(rdb)
	_, ok, err := q.Dequeue(context.Background(), 200*time.Millisecond)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatal("expected timeout (ok=false) on empty queue")
	}
}

func TestQueue_Remove(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	q := New(rdb)
	ctx := context.Background()
	_ = q.Enqueue(ctx, usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal})
	if err := q.Remove(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	depth, _ := q.Depth(ctx, domain.PriorityNormal)
	if depth != 0 {
		t.Fatalf("want depth 0, got %d", depth)
	}
}

func TestRetry_DuePromote(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	q := New(rdb)
	rq := NewRetry(rdb)
	ctx := context.Background()

	past := time.Now().Add(-time.Second)
	_ = rq.Schedule(ctx, usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}, past)

	n, err := rq.DuePromote(ctx, time.Now(), q)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1 promoted, got %d", n)
	}
	item, ok, _ := q.Dequeue(ctx, time.Second)
	if !ok || item.NotificationID != "n1" {
		t.Fatal("item not promoted to queue")
	}
}

func TestRetry_FutureNotPromoted(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	q := New(rdb)
	rq := NewRetry(rdb)
	ctx := context.Background()

	future := time.Now().Add(time.Hour)
	_ = rq.Schedule(ctx, usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}, future)

	n, err := rq.DuePromote(ctx, time.Now(), q)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("want 0 promoted, got %d", n)
	}
}

func TestScheduled_RemoveAndPromote(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	q := New(rdb)
	sc := NewScheduled(rdb)
	ctx := context.Background()

	past := time.Now().Add(-time.Second)
	_ = sc.Add(ctx, usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}, past)
	_ = sc.Add(ctx, usecase.QueueItem{NotificationID: "n2", Priority: domain.PriorityNormal}, past)
	if err := sc.Remove(ctx, "n1"); err != nil {
		t.Fatal(err)
	}
	n, err := sc.DuePromote(ctx, time.Now(), q)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1 promoted after remove, got %d", n)
	}
}

func TestDLQ_PushSize(t *testing.T) {
	rdb := testsupport.RedisClient(t)
	d := NewDLQ(rdb)
	ctx := context.Background()
	_ = d.Push(ctx, "n1", "boom")
	size, err := d.Size(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if size != 1 {
		t.Fatalf("want 1, got %d", size)
	}
}
