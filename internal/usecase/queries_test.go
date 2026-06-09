package usecase

import (
	"context"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestListNotifications_Passthrough(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Status: domain.StatusQueued})
	svc := &ListNotifications{Repo: repo}
	out, total, err := svc.Execute(context.Background(), NotificationFilter{Limit: 10})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if total != 1 || len(out) != 1 {
		t.Fatalf("want 1/1 got %d/%d", total, len(out))
	}
}

func TestListNotifications_DefaultsLimit(t *testing.T) {
	repo := newFakeRepo()
	svc := &ListNotifications{Repo: repo}
	// Limit 0 should be normalized; we can't observe it via the fake's return,
	// but Execute must not error and must not panic.
	if _, _, err := svc.Execute(context.Background(), NotificationFilter{Limit: 0}); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestCreateTemplate(t *testing.T) {
	repo := newFakeTemplateRepo()
	svc := &CreateTemplate{Repo: repo, Clock: fixedClock{}, IDGen: func() string { return "t1" }}
	out, err := svc.Execute(context.Background(), "welcome", domain.ChannelEmail, "Hi {{name}}")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.ID != "t1" {
		t.Fatalf("want t1, got %s", out.ID)
	}
}

func TestCreateTemplate_InvalidChannel(t *testing.T) {
	svc := &CreateTemplate{Repo: newFakeTemplateRepo(), Clock: fixedClock{}, IDGen: func() string { return "t1" }}
	if _, err := svc.Execute(context.Background(), "x", "fax", "body"); err == nil {
		t.Fatal("expected invalid channel error")
	}
}
