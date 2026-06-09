package domain

// Channel is the delivery medium for a notification.
type Channel string

// Supported delivery channels.
const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Valid reports whether the channel is one of the supported values.
func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Priority controls queue ordering.
type Priority string

// Supported priorities.
const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Valid reports whether the priority is one of the supported values.
func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// Status is the lifecycle state of a notification.
type Status string

// Notification lifecycle states.
const (
	StatusPending   Status = "pending"
	StatusQueued    Status = "queued"
	StatusSending   Status = "sending"
	StatusDelivered Status = "delivered"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Cancellable reports whether a notification in this status may still be cancelled.
func (s Status) Cancellable() bool {
	return s == StatusPending || s == StatusQueued
}
