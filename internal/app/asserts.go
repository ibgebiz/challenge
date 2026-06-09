package app

import (
	"github.com/ibrahim-bg/notifier/internal/infrastructure/observability"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// *observability.Metrics must satisfy the usecase metrics port.
var _ usecase.MetricsRecorder = (*observability.Metrics)(nil)
