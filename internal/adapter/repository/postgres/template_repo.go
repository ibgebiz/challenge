package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// TemplateRepo persists templates in Postgres.
type TemplateRepo struct{ pool *pgxpool.Pool }

// NewTemplateRepo constructs a TemplateRepo.
func NewTemplateRepo(p *pgxpool.Pool) *TemplateRepo { return &TemplateRepo{pool: p} }

// Create inserts a template.
func (r *TemplateRepo) Create(ctx context.Context, t domain.Template) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO templates (id, name, channel, body, created_at) VALUES ($1,$2,$3,$4,$5)`,
		t.ID, t.Name, string(t.Channel), t.Body, t.CreatedAt)
	return err
}

// Get returns the template with the given id or domain.ErrNotFound.
func (r *TemplateRepo) Get(ctx context.Context, id string) (domain.Template, error) {
	var (
		t  domain.Template
		ch string
	)
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, channel, body, created_at FROM templates WHERE id=$1`, id).
		Scan(&t.ID, &t.Name, &ch, &t.Body, &t.CreatedAt)
	if err != nil {
		if isNoRows(err) {
			return domain.Template{}, domain.ErrNotFound
		}
		return domain.Template{}, err
	}
	t.Channel = domain.Channel(ch)
	return t, nil
}

// List returns all templates, newest first.
func (r *TemplateRepo) List(ctx context.Context) ([]domain.Template, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, name, channel, body, created_at FROM templates ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Template{}
	for rows.Next() {
		var (
			t  domain.Template
			ch string
		)
		if err := rows.Scan(&t.ID, &t.Name, &ch, &t.Body, &t.CreatedAt); err != nil {
			return nil, err
		}
		t.Channel = domain.Channel(ch)
		out = append(out, t)
	}
	return out, rows.Err()
}
