// Package ws provides a WebSocket hub and a Redis pub/sub bridge for delivering
// notification status events to connected clients.
package ws

import (
	"context"
	"sync"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// Hub fans status events out to subscribers keyed by notification id or batch id.
type Hub struct {
	mu   sync.RWMutex
	subs map[string]map[chan usecase.StatusEvent]struct{}
}

// NewHub constructs an empty Hub.
func NewHub() *Hub {
	return &Hub{subs: map[string]map[chan usecase.StatusEvent]struct{}{}}
}

// Subscribe returns a new buffered channel receiving events for the given key
// (a notification id or batch id).
func (h *Hub) Subscribe(key string) chan usecase.StatusEvent {
	ch := make(chan usecase.StatusEvent, 8)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[key] == nil {
		h.subs[key] = map[chan usecase.StatusEvent]struct{}{}
	}
	h.subs[key][ch] = struct{}{}
	return ch
}

// Unsubscribe removes and closes a subscriber channel.
func (h *Hub) Unsubscribe(key string, ch chan usecase.StatusEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m := h.subs[key]; m != nil {
		if _, ok := m[ch]; ok {
			delete(m, ch)
			close(ch)
		}
		if len(m) == 0 {
			delete(h.subs, key)
		}
	}
}

// Publish delivers an event to subscribers of its notification id and batch id.
// Slow consumers are skipped rather than blocking the publisher.
func (h *Hub) Publish(_ context.Context, e usecase.StatusEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.deliver(e.NotificationID, e)
	if e.BatchID != nil {
		h.deliver(*e.BatchID, e)
	}
}

func (h *Hub) deliver(key string, e usecase.StatusEvent) {
	for ch := range h.subs[key] {
		select {
		case ch <- e:
		default: // drop for slow consumers
		}
	}
}
