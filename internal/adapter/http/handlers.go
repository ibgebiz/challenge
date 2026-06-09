package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// Service interfaces keep handlers decoupled from concrete interactors.
type (
	createService interface {
		Execute(context.Context, usecase.CreateInput) (domain.Notification, error)
	}
	batchService interface {
		Execute(context.Context, []usecase.CreateInput) (domain.Batch, error)
	}
	getService interface {
		Execute(context.Context, string) (domain.Notification, error)
	}
	listService interface {
		Execute(context.Context, usecase.NotificationFilter) ([]domain.Notification, int, error)
	}
	cancelService interface {
		Execute(context.Context, string) error
	}
	batchStatusService interface {
		Execute(context.Context, string) (domain.Batch, map[domain.Status]int, error)
	}
	createTemplateService interface {
		Execute(context.Context, string, domain.Channel, string) (domain.Template, error)
	}
	listTemplatesService interface {
		Execute(context.Context) ([]domain.Template, error)
	}
)

// Handlers bundles the HTTP handlers and their service dependencies.
type Handlers struct {
	Create         createService
	CreateBatchSvc batchService
	Get            getService
	List           listService
	Cancel         cancelService
	BatchStatus    batchStatusService
	CreateTemplate createTemplateService
	ListTemplates  listTemplatesService
}

// IdempotencyHeader carries a client-supplied idempotency key for single creates.
const IdempotencyHeader = "Idempotency-Key"

// CreateNotification handles POST /api/v1/notifications.
func (h *Handlers) CreateNotification(c *gin.Context) {
	var req CreateNotificationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrValidation, err.Error())
		return
	}
	in := req.toCreateInput()
	if key := c.GetHeader(IdempotencyHeader); key != "" {
		in.IdempotencyKey = &key
	}
	n, err := h.Create.Execute(c.Request.Context(), in)
	if err != nil {
		writeError(c, err, "")
		return
	}
	c.JSON(http.StatusCreated, notificationToResponse(n))
}

// CreateBatch handles POST /api/v1/notifications/batch.
func (h *Handlers) CreateBatch(c *gin.Context) {
	var req BatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrValidation, err.Error())
		return
	}
	items := make([]usecase.CreateInput, len(req.Notifications))
	for i, n := range req.Notifications {
		items[i] = n.toCreateInput()
	}
	b, err := h.CreateBatchSvc.Execute(c.Request.Context(), items)
	if err != nil {
		writeError(c, err, "")
		return
	}
	c.JSON(http.StatusCreated, BatchResponse{ID: b.ID, Total: b.Total})
}

// GetNotification handles GET /api/v1/notifications/:id.
func (h *Handlers) GetNotification(c *gin.Context) {
	n, err := h.Get.Execute(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeError(c, err, "")
		return
	}
	c.JSON(http.StatusOK, notificationToResponse(n))
}

// ListNotifications handles GET /api/v1/notifications.
func (h *Handlers) ListNotifications(c *gin.Context) {
	f, err := parseFilter(c)
	if err != nil {
		writeError(c, domain.ErrValidation, err.Error())
		return
	}
	items, total, err := h.List.Execute(c.Request.Context(), f)
	if err != nil {
		writeError(c, err, "")
		return
	}
	resp := ListResponse{Total: total, Limit: f.Limit, Offset: f.Offset}
	for _, n := range items {
		resp.Items = append(resp.Items, notificationToResponse(n))
	}
	c.JSON(http.StatusOK, resp)
}

// CancelNotification handles DELETE /api/v1/notifications/:id.
func (h *Handlers) CancelNotification(c *gin.Context) {
	if err := h.Cancel.Execute(c.Request.Context(), c.Param("id")); err != nil {
		writeError(c, err, "")
		return
	}
	c.Status(http.StatusNoContent)
}

// GetBatch handles GET /api/v1/batches/:id.
func (h *Handlers) GetBatch(c *gin.Context) {
	b, counts, err := h.BatchStatus.Execute(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeError(c, err, "")
		return
	}
	c.JSON(http.StatusOK, BatchStatusResponse{ID: b.ID, Total: b.Total, Counts: counts})
}

// CreateTemplateHandler handles POST /api/v1/templates.
func (h *Handlers) CreateTemplateHandler(c *gin.Context) {
	var req CreateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, domain.ErrValidation, err.Error())
		return
	}
	t, err := h.CreateTemplate.Execute(c.Request.Context(), req.Name, req.Channel, req.Body)
	if err != nil {
		writeError(c, err, "")
		return
	}
	c.JSON(http.StatusCreated, templateToResponse(t))
}

// ListTemplatesHandler handles GET /api/v1/templates.
func (h *Handlers) ListTemplatesHandler(c *gin.Context) {
	ts, err := h.ListTemplates.Execute(c.Request.Context())
	if err != nil {
		writeError(c, err, "")
		return
	}
	out := make([]TemplateResponse, 0, len(ts))
	for _, t := range ts {
		out = append(out, templateToResponse(t))
	}
	c.JSON(http.StatusOK, out)
}

func parseFilter(c *gin.Context) (usecase.NotificationFilter, error) {
	var f usecase.NotificationFilter
	if s := c.Query("status"); s != "" {
		st := domain.Status(s)
		f.Status = &st
	}
	if ch := c.Query("channel"); ch != "" {
		cc := domain.Channel(ch)
		f.Channel = &cc
	}
	if v := c.Query("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, err
		}
		f.From = &t
	}
	if v := c.Query("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, err
		}
		f.To = &t
	}
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return f, err
		}
		f.Limit = n
	}
	if v := c.Query("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return f, err
		}
		f.Offset = n
	}
	return f, nil
}

// writeError maps a domain error to an HTTP status and JSON error body.
func writeError(c *gin.Context, err error, detail string) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, domain.ErrValidation):
		status = http.StatusBadRequest
	case errors.Is(err, domain.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, domain.ErrNotCancellable), errors.Is(err, domain.ErrDuplicate):
		status = http.StatusConflict
	}
	msg := err.Error()
	if detail != "" {
		msg = detail
	}
	c.JSON(status, gin.H{"error": msg})
}
