package postgres

import "github.com/ibrahim-bg/notifier/internal/usecase"

// Compile-time checks that repositories satisfy the usecase ports.
var (
	_ usecase.NotificationRepository    = (*NotificationRepo)(nil)
	_ usecase.TemplateRepository        = (*TemplateRepo)(nil)
	_ usecase.BatchRepository           = (*BatchRepo)(nil)
	_ usecase.DeliveryAttemptRepository = (*AttemptRepo)(nil)
)
