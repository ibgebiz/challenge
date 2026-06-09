package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func newCreateSvc(repo *fakeNotifRepo, q *fakeQueue, idem *fakeIdem, tmpl *fakeTemplateRepo) *CreateNotification {
	return &CreateNotification{
		Repo:      repo,
		Queue:     q,
		Idem:      idem,
		Templates: tmpl,
		Sched:     newFakeScheduled(),
		Clock:     fixedClock{t: time.Unix(1000, 0)},
		IDGen:     func() string { return "fixed-id" },
	}
}

func TestCreate_EnqueuesImmediate(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	out, err := svc.Execute(context.Background(), CreateInput{
		Channel: domain.ChannelSMS, Recipient: "+90555", Content: "hi", Priority: domain.PriorityHigh,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.Status != domain.StatusQueued {
		t.Fatalf("want queued, got %s", out.Status)
	}
	if got, _ := q.len(domain.PriorityHigh); got != 1 {
		t.Fatalf("want 1 queued, got %d", got)
	}
}

func TestCreate_IdempotentReturnsExisting(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	key := "k1"
	in := CreateInput{Channel: domain.ChannelSMS, Recipient: "+90555", Content: "hi", Priority: domain.PriorityNormal, IdempotencyKey: &key}
	first, _ := svc.Execute(context.Background(), in)
	second, err := svc.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("ids differ: %s vs %s", first.ID, second.ID)
	}
	if got, _ := q.len(domain.PriorityNormal); got != 1 {
		t.Fatalf("want 1 enqueue, got %d", got)
	}
}

func TestCreate_ScheduledNotEnqueued(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	future := time.Unix(2000, 0)
	out, err := svc.Execute(context.Background(), CreateInput{
		Channel: domain.ChannelSMS, Recipient: "+90555", Content: "hi", Priority: domain.PriorityLow, ScheduledAt: &future,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.Status != domain.StatusPending {
		t.Fatalf("want pending, got %s", out.Status)
	}
	if got, _ := q.len(domain.PriorityLow); got != 0 {
		t.Fatalf("want 0 enqueue, got %d", got)
	}
}

func TestCreate_RendersTemplate(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	_ = tmpl.Create(context.Background(), domain.Template{ID: "t1", Channel: domain.ChannelSMS, Body: "Hi {{name}}"})
	svc := newCreateSvc(repo, q, idem, tmpl)
	tid := "t1"
	out, err := svc.Execute(context.Background(), CreateInput{
		Channel: domain.ChannelSMS, Recipient: "+1", Priority: domain.PriorityNormal,
		TemplateID: &tid, Variables: map[string]string{"name": "Ada"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.Content != "Hi Ada" {
		t.Fatalf("want rendered content, got %q", out.Content)
	}
}

func TestCreate_InvalidChannelRejected(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	_, err := svc.Execute(context.Background(), CreateInput{
		Channel: "fax", Recipient: "+1", Content: "hi", Priority: domain.PriorityNormal,
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}
