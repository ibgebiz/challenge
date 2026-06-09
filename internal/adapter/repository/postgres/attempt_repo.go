package postgres

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// AttemptRepo records delivery attempts in Postgres.
type AttemptRepo struct{ pool *pgxpool.Pool }

// NewAttemptRepo constructs an AttemptRepo.
func NewAttemptRepo(p *pgxpool.Pool) *AttemptRepo { return &AttemptRepo{pool: p} }

// Add inserts a delivery attempt record.
func (r *AttemptRepo) Add(ctx context.Context, a domain.DeliveryAttempt) error {
	var resp []byte
	if a.ProviderResponse != nil {
		b, err := json.Marshal(a.ProviderResponse)
		if err != nil {
			return err
		}
		resp = b
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO delivery_attempts
		(id, notification_id, attempt_no, status, provider_response, error, attempted_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		a.ID, a.NotificationID, a.AttemptNo, a.Status, resp, a.Error, a.AttemptedAt)
	return err
}
