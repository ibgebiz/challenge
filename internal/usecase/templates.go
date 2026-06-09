package usecase

import (
	"context"
	"fmt"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// CreateTemplate persists a new message template.
type CreateTemplate struct {
	Repo  TemplateRepository
	Clock Clock
	IDGen func() string
}

// Execute validates the channel and stores the template.
func (s *CreateTemplate) Execute(ctx context.Context, name string, ch domain.Channel, body string) (domain.Template, error) {
	if !ch.Valid() {
		return domain.Template{}, fmt.Errorf("%w: invalid channel %q", domain.ErrValidation, ch)
	}
	if name == "" || body == "" {
		return domain.Template{}, fmt.Errorf("%w: name and body required", domain.ErrValidation)
	}
	t := domain.Template{ID: s.IDGen(), Name: name, Channel: ch, Body: body, CreatedAt: s.Clock.Now()}
	return t, s.Repo.Create(ctx, t)
}

// ListTemplates returns all templates.
type ListTemplates struct{ Repo TemplateRepository }

// Execute returns every stored template.
func (s *ListTemplates) Execute(ctx context.Context) ([]domain.Template, error) {
	return s.Repo.List(ctx)
}
