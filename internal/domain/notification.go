// Package domain holds enterprise entities and rules with no external dependencies.
package domain

import "time"

// Notification is the core entity representing a single message to deliver.
type Notification struct {
	ID                string
	BatchID           *string
	Channel           Channel
	Recipient         string
	Content           string
	TemplateID        *string
	Variables         map[string]string
	Priority          Priority
	Status            Status
	IdempotencyKey    *string
	ScheduledAt       *time.Time
	Attempts          int
	LastError         *string
	ProviderMessageID *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Batch groups notifications created in a single batch request.
type Batch struct {
	ID        string
	Total     int
	CreatedAt time.Time
}

// DeliveryAttempt records a single attempt to deliver a notification.
type DeliveryAttempt struct {
	ID               string
	NotificationID   string
	AttemptNo        int
	Status           string
	ProviderResponse map[string]any
	Error            *string
	AttemptedAt      time.Time
}

// Template is a reusable message body with {{variable}} placeholders.
type Template struct {
	ID        string
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
}
