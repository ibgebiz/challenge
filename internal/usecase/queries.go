package usecase

import (
	"context"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

const defaultListLimit = 50

// ListNotifications returns a filtered, paginated list of notifications.
type ListNotifications struct{ Repo NotificationRepository }

// Execute applies sane pagination bounds then delegates to the repository.
func (s *ListNotifications) Execute(ctx context.Context, f NotificationFilter) ([]domain.Notification, int, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = defaultListLimit
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	return s.Repo.List(ctx, f)
}

// GetNotification fetches a single notification by id.
type GetNotification struct{ Repo NotificationRepository }

// Execute returns the notification or domain.ErrNotFound.
func (s *GetNotification) Execute(ctx context.Context, id string) (domain.Notification, error) {
	return s.Repo.Get(ctx, id)
}

// GetBatchStatus returns a batch record and its per-status counts.
type GetBatchStatus struct{ Batches BatchRepository }

// Execute returns the batch plus a map of status to count.
func (s *GetBatchStatus) Execute(ctx context.Context, id string) (domain.Batch, map[domain.Status]int, error) {
	b, err := s.Batches.Get(ctx, id)
	if err != nil {
		return domain.Batch{}, nil, err
	}
	counts, err := s.Batches.StatusCounts(ctx, id)
	return b, counts, err
}
