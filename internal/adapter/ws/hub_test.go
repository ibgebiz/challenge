package ws

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func TestHub_PublishToSubscriber(t *testing.T) {
	h := NewHub()
	ch := h.Subscribe("n1")
	defer h.Unsubscribe("n1", ch)

	h.Publish(context.Background(), usecase.StatusEvent{NotificationID: "n1", Status: domain.StatusDelivered, At: time.Now()})

	select {
	case e := <-ch:
		if e.Status != domain.StatusDelivered {
			t.Fatalf("got %s", e.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}
}

func TestHub_PublishByBatchID(t *testing.T) {
	h := NewHub()
	ch := h.Subscribe("batch-1")
	defer h.Unsubscribe("batch-1", ch)

	batch := "batch-1"
	h.Publish(context.Background(), usecase.StatusEvent{NotificationID: "n1", BatchID: &batch, Status: domain.StatusQueued, At: time.Now()})

	select {
	case e := <-ch:
		if e.NotificationID != "n1" {
			t.Fatalf("got %s", e.NotificationID)
		}
	case <-time.After(time.Second):
		t.Fatal("no event received for batch subscriber")
	}
}

func TestHub_UnsubscribeStopsDelivery(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("publish after unsubscribe panicked: %v", r)
		}
	}()
	h := NewHub()
	ch := h.Subscribe("n1")
	h.Unsubscribe("n1", ch)
	// Publishing after unsubscribe must not panic (channel is closed).
	h.Publish(context.Background(), usecase.StatusEvent{NotificationID: "n1", Status: domain.StatusDelivered})
}
