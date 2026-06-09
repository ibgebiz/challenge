// Package postgres implements the usecase repository ports against PostgreSQL.
package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// NotificationRepo persists notifications in Postgres.
type NotificationRepo struct{ pool *pgxpool.Pool }

// NewNotificationRepo constructs a NotificationRepo.
func NewNotificationRepo(p *pgxpool.Pool) *NotificationRepo { return &NotificationRepo{pool: p} }

const insertNotif = `
INSERT INTO notifications
(id, batch_id, channel, recipient, content, template_id, variables, priority, status,
 idempotency_key, scheduled_at, attempts, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`

// Create inserts a single notification, mapping a unique-key violation to
// domain.ErrDuplicate.
func (r *NotificationRepo) Create(ctx context.Context, n domain.Notification) error {
	vars, err := marshalJSON(n.Variables)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, insertNotif,
		n.ID, n.BatchID, string(n.Channel), n.Recipient, n.Content, n.TemplateID, vars,
		string(n.Priority), string(n.Status), n.IdempotencyKey, n.ScheduledAt, n.Attempts,
		n.CreatedAt, n.UpdatedAt)
	if isUniqueViolation(err) {
		return domain.ErrDuplicate
	}
	return err
}

// CreateBatch inserts the batch record and all its notifications in one transaction.
func (r *NotificationRepo) CreateBatch(ctx context.Context, b domain.Batch, ns []domain.Notification) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `INSERT INTO batches (id, total, created_at) VALUES ($1,$2,$3)`,
		b.ID, b.Total, b.CreatedAt); err != nil {
		return err
	}
	for _, n := range ns {
		vars, err := marshalJSON(n.Variables)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, insertNotif,
			n.ID, n.BatchID, string(n.Channel), n.Recipient, n.Content, n.TemplateID, vars,
			string(n.Priority), string(n.Status), n.IdempotencyKey, n.ScheduledAt, n.Attempts,
			n.CreatedAt, n.UpdatedAt); err != nil {
			if isUniqueViolation(err) {
				return domain.ErrDuplicate
			}
			return err
		}
	}
	return tx.Commit(ctx)
}

// Get returns the notification with the given id or domain.ErrNotFound.
func (r *NotificationRepo) Get(ctx context.Context, id string) (domain.Notification, error) {
	return r.scanOne(ctx, `SELECT `+notifCols+` FROM notifications WHERE id=$1`, id)
}

// GetByIdempotencyKey returns the notification with the given key or domain.ErrNotFound.
func (r *NotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error) {
	return r.scanOne(ctx, `SELECT `+notifCols+` FROM notifications WHERE idempotency_key=$1`, key)
}

// UpdateStatus updates the status and related fields. provider_message_id is only
// overwritten when a non-nil value is supplied.
func (r *NotificationRepo) UpdateStatus(ctx context.Context, id string, s domain.Status, lastErr, providerMsgID *string, attempts int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE notifications
		SET status=$2, last_error=$3,
		    provider_message_id=COALESCE($4, provider_message_id),
		    attempts=$5, updated_at=now()
		WHERE id=$1`, id, string(s), lastErr, providerMsgID, attempts)
	return err
}

// List returns a filtered, paginated slice of notifications and the total count
// matching the filter (ignoring pagination).
func (r *NotificationRepo) List(ctx context.Context, f usecase.NotificationFilter) ([]domain.Notification, int, error) {
	where := "WHERE 1=1"
	args := []any{}
	if f.Status != nil {
		args = append(args, string(*f.Status))
		where += " AND status=$" + itoa(len(args))
	}
	if f.Channel != nil {
		args = append(args, string(*f.Channel))
		where += " AND channel=$" + itoa(len(args))
	}
	if f.From != nil {
		args = append(args, *f.From)
		where += " AND created_at>=$" + itoa(len(args))
	}
	if f.To != nil {
		args = append(args, *f.To)
		where += " AND created_at<=$" + itoa(len(args))
	}

	var total int
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM notifications `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	q := `SELECT ` + notifCols + ` FROM notifications ` + where +
		` ORDER BY created_at DESC LIMIT $` + itoa(len(args)+1) + ` OFFSET $` + itoa(len(args)+2)
	args = append(args, f.Limit, f.Offset)

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []domain.Notification{}
	for rows.Next() {
		n, err := scanNotif(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, n)
	}
	return out, total, rows.Err()
}

func (r *NotificationRepo) scanOne(ctx context.Context, q string, args ...any) (domain.Notification, error) {
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return domain.Notification{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return domain.Notification{}, err
		}
		return domain.Notification{}, domain.ErrNotFound
	}
	return scanNotif(rows)
}

const notifCols = `id, batch_id, channel, recipient, content, template_id, variables, priority, status,
	idempotency_key, scheduled_at, attempts, last_error, provider_message_id, created_at, updated_at`

func scanNotif(rows pgx.Rows) (domain.Notification, error) {
	var (
		n        domain.Notification
		channel  string
		priority string
		status   string
		varsRaw  []byte
	)
	if err := rows.Scan(&n.ID, &n.BatchID, &channel, &n.Recipient, &n.Content, &n.TemplateID,
		&varsRaw, &priority, &status, &n.IdempotencyKey, &n.ScheduledAt, &n.Attempts,
		&n.LastError, &n.ProviderMessageID, &n.CreatedAt, &n.UpdatedAt); err != nil {
		return domain.Notification{}, err
	}
	n.Channel = domain.Channel(channel)
	n.Priority = domain.Priority(priority)
	n.Status = domain.Status(status)
	if len(varsRaw) > 0 {
		if err := json.Unmarshal(varsRaw, &n.Variables); err != nil {
			return domain.Notification{}, err
		}
	}
	return n, nil
}

func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	// nil maps marshal to "null"; keep them as SQL NULL instead.
	if m, ok := v.(map[string]string); ok && m == nil {
		return nil, nil
	}
	return json.Marshal(v)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
