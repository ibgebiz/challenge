package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// ErrRateLimited is returned when no rate-limit token was available and the item
// has been re-queued for a later attempt.
var ErrRateLimited = errors.New("rate limited")

// ProcessNotification delivers a single notification, recording attempts and
// applying retry/DLQ logic on failure.
type ProcessNotification struct {
	Repo          NotificationRepository
	Provider      Provider
	RateLimiter   RateLimiter
	Attempts      DeliveryAttemptRepository
	Retry         RetryQueue
	DLQ           DLQ
	Queue         Queue
	Publisher     EventPublisher
	Metrics       MetricsRecorder
	Clock         Clock
	MaxAttempts   int
	RetryInterval time.Duration
	IDGen         func() string
}

// Execute processes one queue item: it rate-limits, sends via the provider, and
// records the outcome. On failure it schedules a retry or dead-letters the item.
func (s *ProcessNotification) Execute(ctx context.Context, item QueueItem) error {
	n, err := s.Repo.Get(ctx, item.NotificationID)
	if err != nil {
		return err
	}
	if n.Status == domain.StatusCancelled || n.Status == domain.StatusDelivered {
		return nil // nothing to do
	}

	allowed, err := s.RateLimiter.Allow(ctx, n.Channel)
	if err != nil {
		return err
	}
	if !allowed {
		// Re-queue with a small delay so the worker stays free for other items.
		_ = s.Retry.Schedule(ctx, item, s.Clock.Now().Add(time.Second))
		s.record(func(m MetricsRecorder) { m.IncRateLimited(n.Channel) })
		return ErrRateLimited
	}

	_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusSending, n.LastError, n.ProviderMessageID, n.Attempts)
	s.publish(ctx, n, domain.StatusSending)

	attemptNo := n.Attempts + 1
	start := s.Clock.Now()
	resp, sendErr := s.Provider.Send(ctx, n)
	latency := s.Clock.Now().Sub(start).Seconds()

	att := domain.DeliveryAttempt{
		ID: s.IDGen(), NotificationID: n.ID, AttemptNo: attemptNo, AttemptedAt: s.Clock.Now(),
	}
	if sendErr != nil {
		msg := sendErr.Error()
		att.Status = "failed"
		att.Error = &msg
		_ = s.Attempts.Add(ctx, att)
		return s.handleFailure(ctx, n, item, attemptNo, msg)
	}

	att.Status = "delivered"
	att.ProviderResponse = map[string]any{
		"messageId": resp.MessageID, "status": resp.Status, "timestamp": resp.Timestamp,
	}
	_ = s.Attempts.Add(ctx, att)
	_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusDelivered, nil, &resp.MessageID, attemptNo)
	s.publish(ctx, n, domain.StatusDelivered)
	s.record(func(m MetricsRecorder) {
		m.IncDelivered(n.Channel)
		m.ObserveLatency(n.Channel, latency)
	})
	return nil
}

func (s *ProcessNotification) handleFailure(ctx context.Context, n domain.Notification, item QueueItem, attemptNo int, reason string) error {
	if attemptNo >= s.MaxAttempts {
		_ = s.DLQ.Push(ctx, n.ID, reason)
		_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusFailed, &reason, nil, attemptNo)
		s.publish(ctx, n, domain.StatusFailed)
		s.record(func(m MetricsRecorder) { m.IncFailed(n.Channel) })
		return nil
	}
	_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusQueued, &reason, nil, attemptNo)
	s.record(func(m MetricsRecorder) { m.IncRetried(n.Channel) })
	return s.Retry.Schedule(ctx, item, s.Clock.Now().Add(s.RetryInterval))
}

func (s *ProcessNotification) publish(ctx context.Context, n domain.Notification, st domain.Status) {
	s.Publisher.Publish(ctx, StatusEvent{NotificationID: n.ID, BatchID: n.BatchID, Status: st, At: s.Clock.Now()})
}

func (s *ProcessNotification) record(fn func(MetricsRecorder)) {
	if s.Metrics != nil {
		fn(s.Metrics)
	}
}
