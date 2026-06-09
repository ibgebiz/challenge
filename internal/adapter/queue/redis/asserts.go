package redisqueue

import "github.com/ibrahim-bg/notifier/internal/usecase"

// Compile-time checks that Redis adapters satisfy the usecase ports.
var (
	_ usecase.Queue          = (*Queue)(nil)
	_ usecase.RetryQueue     = (*Retry)(nil)
	_ usecase.DLQ            = (*DLQ)(nil)
	_ usecase.ScheduledStore = (*Scheduled)(nil)
)
