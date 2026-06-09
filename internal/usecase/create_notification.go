package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// CreateInput describes a single notification creation request.
type CreateInput struct {
	Channel        domain.Channel
	Recipient      string
	Content        string
	TemplateID     *string
	Variables      map[string]string
	Priority       domain.Priority
	IdempotencyKey *string
	ScheduledAt    *time.Time
	BatchID        *string
}

// CreateNotification validates, persists, and enqueues (or schedules) a notification.
type CreateNotification struct {
	Repo      NotificationRepository
	Queue     Queue
	Sched     ScheduledStore
	Idem      IdempotencyStore
	Templates TemplateRepository
	Clock     Clock
	IDGen     func() string
}

// Execute creates a notification. If an idempotency key matches an existing
// notification, that one is returned without creating or sending a duplicate.
func (s *CreateNotification) Execute(ctx context.Context, in CreateInput) (domain.Notification, error) {
	if in.Priority == "" {
		in.Priority = domain.PriorityNormal
	}

	if in.IdempotencyKey != nil {
		if existing, err := s.Repo.GetByIdempotencyKey(ctx, *in.IdempotencyKey); err == nil {
			return existing, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.Notification{}, err
		}
	}

	content := in.Content
	if in.TemplateID != nil {
		tmpl, err := s.Templates.Get(ctx, *in.TemplateID)
		if err != nil {
			return domain.Notification{}, err
		}
		rendered, err := domain.Render(tmpl.Body, in.Variables)
		if err != nil {
			return domain.Notification{}, err
		}
		content = rendered
	}

	now := s.Clock.Now()
	n := domain.Notification{
		ID:             s.IDGen(),
		BatchID:        in.BatchID,
		Channel:        in.Channel,
		Recipient:      in.Recipient,
		Content:        content,
		TemplateID:     in.TemplateID,
		Variables:      in.Variables,
		Priority:       in.Priority,
		Status:         domain.StatusPending,
		IdempotencyKey: in.IdempotencyKey,
		ScheduledAt:    in.ScheduledAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := domain.ValidateNotification(n); err != nil {
		return domain.Notification{}, err
	}

	// Claim the idempotency key. The DB unique constraint is the ultimate source
	// of truth; this guards against concurrent duplicates before the insert.
	if in.IdempotencyKey != nil {
		if existing, found, err := s.Idem.Remember(ctx, *in.IdempotencyKey, n.ID); err != nil {
			return domain.Notification{}, err
		} else if found {
			return s.Repo.Get(ctx, existing)
		}
	}

	if err := s.Repo.Create(ctx, n); err != nil {
		return domain.Notification{}, err
	}

	item := QueueItem{NotificationID: n.ID, Priority: n.Priority}
	if in.ScheduledAt != nil && in.ScheduledAt.After(now) {
		if err := s.Sched.Add(ctx, item, *in.ScheduledAt); err != nil {
			return domain.Notification{}, err
		}
		return n, nil // remains pending until the scheduler promotes it
	}

	if err := s.Queue.Enqueue(ctx, item); err != nil {
		return domain.Notification{}, err
	}
	n.Status = domain.StatusQueued
	if err := s.Repo.UpdateStatus(ctx, n.ID, domain.StatusQueued, nil, nil, 0); err != nil {
		return domain.Notification{}, err
	}
	return n, nil
}
