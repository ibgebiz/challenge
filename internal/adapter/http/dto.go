package httpapi

import (
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// CreateNotificationRequest is the body for creating a single notification.
type CreateNotificationRequest struct {
	Channel        domain.Channel    `json:"channel" binding:"required"`
	Recipient      string            `json:"recipient" binding:"required"`
	Content        string            `json:"content"`
	TemplateID     *string           `json:"templateId"`
	Variables      map[string]string `json:"variables"`
	Priority       domain.Priority   `json:"priority"`
	IdempotencyKey *string           `json:"idempotencyKey"`
	ScheduledAt    *time.Time        `json:"scheduledAt"`
}

func (r CreateNotificationRequest) toCreateInput() usecase.CreateInput {
	return usecase.CreateInput{
		Channel:        r.Channel,
		Recipient:      r.Recipient,
		Content:        r.Content,
		TemplateID:     r.TemplateID,
		Variables:      r.Variables,
		Priority:       r.Priority,
		IdempotencyKey: r.IdempotencyKey,
		ScheduledAt:    r.ScheduledAt,
	}
}

// BatchRequest is the body for creating many notifications at once.
type BatchRequest struct {
	Notifications []CreateNotificationRequest `json:"notifications" binding:"required"`
}

// CreateTemplateRequest is the body for creating a template.
type CreateTemplateRequest struct {
	Name    string         `json:"name" binding:"required"`
	Channel domain.Channel `json:"channel" binding:"required"`
	Body    string         `json:"body" binding:"required"`
}

// NotificationResponse is the API representation of a notification.
type NotificationResponse struct {
	ID                string            `json:"id"`
	BatchID           *string           `json:"batchId,omitempty"`
	Channel           domain.Channel    `json:"channel"`
	Recipient         string            `json:"recipient"`
	Content           string            `json:"content"`
	TemplateID        *string           `json:"templateId,omitempty"`
	Variables         map[string]string `json:"variables,omitempty"`
	Priority          domain.Priority   `json:"priority"`
	Status            domain.Status     `json:"status"`
	ScheduledAt       *time.Time        `json:"scheduledAt,omitempty"`
	Attempts          int               `json:"attempts"`
	LastError         *string           `json:"lastError,omitempty"`
	ProviderMessageID *string           `json:"providerMessageId,omitempty"`
	CreatedAt         time.Time         `json:"createdAt"`
	UpdatedAt         time.Time         `json:"updatedAt"`
}

func notificationToResponse(n domain.Notification) NotificationResponse {
	return NotificationResponse{
		ID: n.ID, BatchID: n.BatchID, Channel: n.Channel, Recipient: n.Recipient,
		Content: n.Content, TemplateID: n.TemplateID, Variables: n.Variables,
		Priority: n.Priority, Status: n.Status, ScheduledAt: n.ScheduledAt,
		Attempts: n.Attempts, LastError: n.LastError, ProviderMessageID: n.ProviderMessageID,
		CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt,
	}
}

// ListResponse is a paginated list of notifications.
type ListResponse struct {
	Items  []NotificationResponse `json:"items"`
	Total  int                    `json:"total"`
	Limit  int                    `json:"limit"`
	Offset int                    `json:"offset"`
}

// BatchResponse is returned after creating a batch.
type BatchResponse struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// BatchStatusResponse summarizes a batch's per-status counts.
type BatchStatusResponse struct {
	ID     string                `json:"id"`
	Total  int                   `json:"total"`
	Counts map[domain.Status]int `json:"counts"`
}

// TemplateResponse is the API representation of a template.
type TemplateResponse struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Channel   domain.Channel `json:"channel"`
	Body      string         `json:"body"`
	CreatedAt time.Time      `json:"createdAt"`
}

func templateToResponse(t domain.Template) TemplateResponse {
	return TemplateResponse{ID: t.ID, Name: t.Name, Channel: t.Channel, Body: t.Body, CreatedAt: t.CreatedAt}
}
