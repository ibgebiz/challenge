package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// BatchRepo reads batch records and aggregate status counts.
type BatchRepo struct{ pool *pgxpool.Pool }

// NewBatchRepo constructs a BatchRepo.
func NewBatchRepo(p *pgxpool.Pool) *BatchRepo { return &BatchRepo{pool: p} }

// Create inserts a batch record.
func (r *BatchRepo) Create(ctx context.Context, b domain.Batch) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO batches (id, total, created_at) VALUES ($1,$2,$3)`,
		b.ID, b.Total, b.CreatedAt)
	return err
}

// Get returns the batch with the given id or domain.ErrNotFound.
func (r *BatchRepo) Get(ctx context.Context, id string) (domain.Batch, error) {
	var b domain.Batch
	err := r.pool.QueryRow(ctx,
		`SELECT id, total, created_at FROM batches WHERE id=$1`, id).
		Scan(&b.ID, &b.Total, &b.CreatedAt)
	if err != nil {
		if isNoRows(err) {
			return domain.Batch{}, domain.ErrNotFound
		}
		return domain.Batch{}, err
	}
	return b, nil
}

// StatusCounts returns the number of notifications in each status for a batch.
func (r *BatchRepo) StatusCounts(ctx context.Context, batchID string) (map[domain.Status]int, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id=$1 GROUP BY status`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[domain.Status]int{}
	for rows.Next() {
		var (
			s string
			c int
		)
		if err := rows.Scan(&s, &c); err != nil {
			return nil, err
		}
		counts[domain.Status(s)] = c
	}
	return counts, rows.Err()
}
