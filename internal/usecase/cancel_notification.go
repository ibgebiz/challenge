package usecase

import (
	"context"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// CancelNotification cancels a pending or queued notification and removes it
// from the queue and scheduled store.
type CancelNotification struct {
	Repo  NotificationRepository
	Queue Queue
	Sched ScheduledStore
	Clock Clock
}

// Execute cancels the notification with the given id. In-flight (sending),
// delivered, failed, or already-cancelled notifications cannot be cancelled.
func (s *CancelNotification) Execute(ctx context.Context, id string) error {
	n, err := s.Repo.Get(ctx, id)
	if err != nil {
		return err
	}
	if !n.Status.Cancellable() {
		return domain.ErrNotCancellable
	}
	_ = s.Queue.Remove(ctx, id)
	_ = s.Sched.Remove(ctx, id)
	return s.Repo.UpdateStatus(ctx, id, domain.StatusCancelled, nil, nil, n.Attempts)
}
