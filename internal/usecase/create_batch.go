package usecase

import (
	"context"
	"fmt"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// MaxBatchSize is the maximum number of notifications allowed in one batch request.
const MaxBatchSize = 1000

// CreateBatch creates many notifications under a shared batch id.
type CreateBatch struct {
	Create  *CreateNotification
	Batches BatchRepository
	Clock   Clock
	IDGen   func() string
}

// Execute validates the batch size, persists the batch record, assigns the batch
// id to every item, and creates each notification (reusing CreateNotification so
// templates, idempotency, scheduling, and validation all apply per item).
func (s *CreateBatch) Execute(ctx context.Context, items []CreateInput) (domain.Batch, error) {
	if len(items) == 0 {
		return domain.Batch{}, fmt.Errorf("%w: batch is empty", domain.ErrValidation)
	}
	if len(items) > MaxBatchSize {
		return domain.Batch{}, fmt.Errorf("%w: batch exceeds %d", domain.ErrValidation, MaxBatchSize)
	}
	batchID := s.IDGen()
	batch := domain.Batch{ID: batchID, Total: len(items), CreatedAt: s.Clock.Now()}
	// Persist the batch row first so notification batch_id FKs are satisfied.
	if err := s.Batches.Create(ctx, batch); err != nil {
		return domain.Batch{}, err
	}
	for i := range items {
		items[i].BatchID = &batchID
		if _, err := s.Create.Execute(ctx, items[i]); err != nil {
			return domain.Batch{}, fmt.Errorf("item %d: %w", i, err)
		}
	}
	return batch, nil
}
