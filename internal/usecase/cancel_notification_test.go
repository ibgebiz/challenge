package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestCancel_QueuedSucceeds(t *testing.T) {
	repo, q := newFakeRepo(), newFakeQueue()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Status: domain.StatusQueued, Priority: domain.PriorityNormal})
	svc := &CancelNotification{Repo: repo, Queue: q, Sched: newFakeScheduled(), Clock: fixedClock{t: time.Unix(1, 0)}}
	if err := svc.Execute(context.Background(), "n1"); err != nil {
		t.Fatalf("err: %v", err)
	}
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusCancelled {
		t.Fatalf("want cancelled, got %s", n.Status)
	}
}

func TestCancel_SendingFails(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n2", Status: domain.StatusSending})
	svc := &CancelNotification{Repo: repo, Queue: newFakeQueue(), Sched: newFakeScheduled(), Clock: fixedClock{t: time.Unix(1, 0)}}
	if err := svc.Execute(context.Background(), "n2"); err == nil {
		t.Fatal("expected not-cancellable error")
	}
}

func TestCancel_NotFound(t *testing.T) {
	svc := &CancelNotification{Repo: newFakeRepo(), Queue: newFakeQueue(), Sched: newFakeScheduled(), Clock: fixedClock{t: time.Unix(1, 0)}}
	if err := svc.Execute(context.Background(), "missing"); err == nil {
		t.Fatal("expected not-found error")
	}
}
