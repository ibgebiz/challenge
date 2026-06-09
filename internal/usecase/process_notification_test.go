package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func newProcessSvc(repo *fakeNotifRepo, prov *fakeProvider, rl *fakeRateLimiter) *ProcessNotification {
	return &ProcessNotification{
		Repo: repo, Provider: prov, RateLimiter: rl, Attempts: newFakeAttempts(),
		Retry: newFakeRetry(), DLQ: newFakeDLQ(), Queue: newFakeQueue(), Publisher: newFakePublisher(),
		Clock: fixedClock{t: time.Unix(1, 0)}, MaxAttempts: 5, RetryInterval: 30 * time.Second,
		IDGen: func() string { return "att" },
	}
}

func TestProcess_Success(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi", Status: domain.StatusQueued})
	prov := &fakeProvider{resp: ProviderResponse{MessageID: "m1", Status: "accepted"}}
	svc := newProcessSvc(repo, prov, &fakeRateLimiter{allow: true})
	if err := svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}); err != nil {
		t.Fatalf("err: %v", err)
	}
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusDelivered {
		t.Fatalf("want delivered, got %s", n.Status)
	}
	if n.ProviderMessageID == nil || *n.ProviderMessageID != "m1" {
		t.Fatal("provider id not stored")
	}
}

func TestProcess_RateLimitedRequeues(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Status: domain.StatusQueued})
	svc := newProcessSvc(repo, &fakeProvider{}, &fakeRateLimiter{allow: false})
	err := svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if size, _ := svc.Retry.Size(context.Background()); size != 1 {
		t.Fatalf("want 1 requeued, got %d", size)
	}
}

func TestProcess_FailureSchedulesRetry(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Status: domain.StatusQueued, Attempts: 0})
	prov := &fakeProvider{err: errors.New("provider down")}
	svc := newProcessSvc(repo, prov, &fakeRateLimiter{allow: true})
	_ = svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal})
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusQueued {
		t.Fatalf("want queued (awaiting retry), got %s", n.Status)
	}
	if size, _ := svc.Retry.Size(context.Background()); size != 1 {
		t.Fatalf("want 1 retry scheduled, got %d", size)
	}
}

func TestProcess_FailureGoesToDLQAtMaxAttempts(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Status: domain.StatusQueued, Attempts: 4})
	prov := &fakeProvider{err: errors.New("provider down")}
	svc := newProcessSvc(repo, prov, &fakeRateLimiter{allow: true})
	_ = svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal})
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusFailed {
		t.Fatalf("want failed (DLQ) after 5th attempt, got %s", n.Status)
	}
	if size, _ := svc.DLQ.Size(context.Background()); size != 1 {
		t.Fatalf("want 1 in DLQ, got %d", size)
	}
}

func TestProcess_SkipsCancelled(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Status: domain.StatusCancelled})
	svc := newProcessSvc(repo, &fakeProvider{}, &fakeRateLimiter{allow: true})
	if err := svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}); err != nil {
		t.Fatalf("err: %v", err)
	}
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusCancelled {
		t.Fatalf("cancelled should stay cancelled, got %s", n.Status)
	}
}
