package observability

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey string

const correlationKey ctxKey = "correlation_id"

// NewLogger returns a JSON slog logger at the given level (e.g. "info", "debug").
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// WithCorrelationID stores a correlation id on the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey, id)
}

// CorrelationID returns the correlation id stored on the context, or "".
func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}
	return ""
}
