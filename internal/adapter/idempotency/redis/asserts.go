package redisidem

import "github.com/ibrahim-bg/notifier/internal/usecase"

var _ usecase.IdempotencyStore = (*Store)(nil)
