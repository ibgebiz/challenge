package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestCreateBatch_RejectsOver1000(t *testing.T) {
	repo := newFakeRepo()
	svc := &CreateBatch{
		Create:  newCreateSvc(repo, newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()),
		Batches: newFakeBatchRepo(), Clock: fixedClock{t: time.Unix(1, 0)}, IDGen: func() string { return "b" },
	}
	items := make([]CreateInput, 1001)
	if _, err := svc.Execute(context.Background(), items); err == nil {
		t.Fatal("expected size error")
	}
}

func TestCreateBatch_RejectsEmpty(t *testing.T) {
	repo := newFakeRepo()
	svc := &CreateBatch{
		Create:  newCreateSvc(repo, newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()),
		Batches: newFakeBatchRepo(), Clock: fixedClock{t: time.Unix(1, 0)}, IDGen: func() string { return "b" },
	}
	if _, err := svc.Execute(context.Background(), nil); err == nil {
		t.Fatal("expected empty-batch error")
	}
}

func TestCreateBatch_CreatesAll(t *testing.T) {
	repo := newFakeRepo()
	svc := &CreateBatch{
		Create:  newCreateSvc(repo, newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()),
		Batches: newFakeBatchRepo(), Clock: fixedClock{t: time.Unix(1, 0)}, IDGen: func() string { return "b" },
	}
	items := []CreateInput{
		{Channel: domain.ChannelSMS, Recipient: "+1", Content: "a", Priority: domain.PriorityNormal},
		{Channel: domain.ChannelEmail, Recipient: "x@y.z", Content: "b", Priority: domain.PriorityNormal},
	}
	out, err := svc.Execute(context.Background(), items)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out.Total != 2 {
		t.Fatalf("want 2, got %d", out.Total)
	}
}
