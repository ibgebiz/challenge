package redisratelimit

import "github.com/ibrahim-bg/notifier/internal/usecase"

var _ usecase.RateLimiter = (*Limiter)(nil)
