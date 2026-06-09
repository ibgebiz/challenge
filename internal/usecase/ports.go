// Package usecase holds application business rules (interactors) and the ports
// (interfaces) they depend on. Outer adapters implement these ports; the cmd/*
// binaries wire concrete implementations in. This layer depends only on domain.
package usecase

import (
	"context"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// NotificationFilter describes list/query criteria with pagination.
type NotificationFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	From    *time.Time
	To      *time.Time
	Limit   int
	Offset  int
}

// QueueItem is the minimal payload carried through queues.
type QueueItem struct {
	NotificationID string
	Priority       domain.Priority
}

// ProviderResponse is the parsed result of a successful provider send.
type ProviderResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// StatusEvent is emitted whenever a notification changes status.
type StatusEvent struct {
	NotificationID string        `json:"notificationId"`
	BatchID        *string       `json:"batchId,omitempty"`
	Status         domain.Status `json:"status"`
	At             time.Time     `json:"at"`
}

// NotificationRepository persists and queries notifications.
type NotificationRepository interface {
	Create(ctx context.Context, n domain.Notification) error
	CreateBatch(ctx context.Context, b domain.Batch, ns []domain.Notification) error
	Get(ctx context.Context, id string) (domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error)
	UpdateStatus(ctx context.Context, id string, status domain.Status, lastErr, providerMsgID *string, attempts int) error
	List(ctx context.Context, f NotificationFilter) ([]domain.Notification, int, error)
}

// BatchRepository persists and reads batch records and aggregate status.
type BatchRepository interface {
	Create(ctx context.Context, b domain.Batch) error
	Get(ctx context.Context, id string) (domain.Batch, error)
	StatusCounts(ctx context.Context, batchID string) (map[domain.Status]int, error)
}

// TemplateRepository persists and queries templates.
type TemplateRepository interface {
	Create(ctx context.Context, t domain.Template) error
	Get(ctx context.Context, id string) (domain.Template, error)
	List(ctx context.Context) ([]domain.Template, error)
}

// DeliveryAttemptRepository records delivery attempts.
type DeliveryAttemptRepository interface {
	Add(ctx context.Context, a domain.DeliveryAttempt) error
}

// Queue is the priority work queue.
type Queue interface {
	Enqueue(ctx context.Context, item QueueItem) error
	// Dequeue blocks up to timeout; returns ok=false on timeout.
	Dequeue(ctx context.Context, timeout time.Duration) (QueueItem, bool, error)
	Remove(ctx context.Context, notificationID string) error
	Depth(ctx context.Context, p domain.Priority) (int64, error)
}

// RetryQueue holds items awaiting a future retry.
type RetryQueue interface {
	Schedule(ctx context.Context, item QueueItem, at time.Time) error
	DuePromote(ctx context.Context, now time.Time, dst Queue) (int, error)
	Size(ctx context.Context) (int64, error)
}

// DLQ is the dead-letter queue for exhausted notifications.
type DLQ interface {
	Push(ctx context.Context, notificationID, reason string) error
	Size(ctx context.Context) (int64, error)
}

// ScheduledStore holds future-dated notifications until they are due.
type ScheduledStore interface {
	Add(ctx context.Context, item QueueItem, at time.Time) error
	Remove(ctx context.Context, notificationID string) error
	DuePromote(ctx context.Context, now time.Time, dst Queue) (int, error)
}

// RateLimiter enforces per-channel throughput limits.
type RateLimiter interface {
	// Allow reports whether a token was available for the channel right now.
	Allow(ctx context.Context, channel domain.Channel) (bool, error)
}

// IdempotencyStore deduplicates create requests by key.
type IdempotencyStore interface {
	// Remember marks key as seen with the given notification id. If the key was
	// already present it returns the existing id and found=true.
	Remember(ctx context.Context, key, notificationID string) (existing string, found bool, err error)
}

// Provider delivers a notification to the external messaging provider.
type Provider interface {
	Send(ctx context.Context, n domain.Notification) (ProviderResponse, error)
}

// EventPublisher broadcasts status events (e.g. to WebSocket subscribers).
type EventPublisher interface {
	Publish(ctx context.Context, e StatusEvent)
}

// Clock abstracts time for deterministic tests.
type Clock interface{ Now() time.Time }

// MetricsRecorder records delivery metrics. Implementations must be safe to call
// concurrently; ProcessNotification treats a nil recorder as a no-op.
type MetricsRecorder interface {
	IncDelivered(channel domain.Channel)
	IncFailed(channel domain.Channel)
	IncRetried(channel domain.Channel)
	IncRateLimited(channel domain.Channel)
	ObserveLatency(channel domain.Channel, seconds float64)
}
