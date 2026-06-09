//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func mkNotif(id string) domain.Notification {
	return domain.Notification{
		ID: id, Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi",
		Priority: domain.PriorityNormal, Status: domain.StatusPending,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
}

func TestNotificationRepo_CreateGet(t *testing.T) {
	pool := newTestPool(t)
	repo := NewNotificationRepo(pool)
	ctx := context.Background()

	n := mkNotif(newUUID())
	n.Variables = map[string]string{"k": "v"}
	if err := repo.Create(ctx, n); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Get(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "hi" {
		t.Fatalf("got %q", got.Content)
	}
	if got.Variables["k"] != "v" {
		t.Fatalf("variables not round-tripped: %v", got.Variables)
	}
}

func TestNotificationRepo_NotFound(t *testing.T) {
	pool := newTestPool(t)
	repo := NewNotificationRepo(pool)
	if _, err := repo.Get(context.Background(), newUUID()); err != domain.ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestNotificationRepo_IdempotencyUnique(t *testing.T) {
	pool := newTestPool(t)
	repo := NewNotificationRepo(pool)
	ctx := context.Background()
	key := "dup"
	mk := func() domain.Notification {
		n := mkNotif(newUUID())
		n.IdempotencyKey = &key
		return n
	}
	if err := repo.Create(ctx, mk()); err != nil {
		t.Fatal(err)
	}
	if err := repo.Create(ctx, mk()); err != domain.ErrDuplicate {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
}

func TestNotificationRepo_UpdateStatusAndList(t *testing.T) {
	pool := newTestPool(t)
	repo := NewNotificationRepo(pool)
	ctx := context.Background()

	n := mkNotif(newUUID())
	if err := repo.Create(ctx, n); err != nil {
		t.Fatal(err)
	}
	mid := "provider-msg-1"
	if err := repo.UpdateStatus(ctx, n.ID, domain.StatusDelivered, nil, &mid, 1); err != nil {
		t.Fatal(err)
	}

	delivered := domain.StatusDelivered
	list, total, err := repo.List(ctx, usecase.NotificationFilter{Status: &delivered, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(list) != 1 {
		t.Fatalf("want 1/1, got %d/%d", total, len(list))
	}
	if list[0].ProviderMessageID == nil || *list[0].ProviderMessageID != mid {
		t.Fatalf("provider message id not stored")
	}
}

func TestBatchRepo_StatusCounts(t *testing.T) {
	pool := newTestPool(t)
	nrepo := NewNotificationRepo(pool)
	brepo := NewBatchRepo(pool)
	ctx := context.Background()

	batchID := newUUID()
	b := domain.Batch{ID: batchID, Total: 2, CreatedAt: time.Now()}
	n1 := mkNotif(newUUID())
	n1.BatchID = &batchID
	n2 := mkNotif(newUUID())
	n2.BatchID = &batchID
	n2.Status = domain.StatusDelivered
	if err := nrepo.CreateBatch(ctx, b, []domain.Notification{n1, n2}); err != nil {
		t.Fatal(err)
	}

	counts, err := brepo.StatusCounts(ctx, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if counts[domain.StatusPending] != 1 || counts[domain.StatusDelivered] != 1 {
		t.Fatalf("unexpected counts: %v", counts)
	}
}
