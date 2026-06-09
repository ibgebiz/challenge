package domain

import "errors"

// Sentinel errors used across layers; adapters map these to transport-level codes.
var (
	ErrValidation     = errors.New("validation failed")
	ErrNotFound       = errors.New("not found")
	ErrNotCancellable = errors.New("notification is not cancellable")
	ErrDuplicate      = errors.New("duplicate idempotency key")
)
