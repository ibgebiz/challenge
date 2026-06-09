package webhook

import "github.com/ibrahim-bg/notifier/internal/usecase"

var _ usecase.Provider = (*Client)(nil)
