package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// stub services -------------------------------------------------------------

type stubCreate struct {
	gotKey string
	err    error
}

func (s *stubCreate) Execute(_ context.Context, in usecase.CreateInput) (domain.Notification, error) {
	if in.IdempotencyKey != nil {
		s.gotKey = *in.IdempotencyKey
	}
	if s.err != nil {
		return domain.Notification{}, s.err
	}
	return domain.Notification{ID: "n1", Channel: in.Channel, Status: domain.StatusQueued}, nil
}

type stubGet struct{ err error }

func (s *stubGet) Execute(_ context.Context, id string) (domain.Notification, error) {
	if s.err != nil {
		return domain.Notification{}, s.err
	}
	return domain.Notification{ID: id, Status: domain.StatusDelivered}, nil
}

type stubCancel struct{ err error }

func (s *stubCancel) Execute(_ context.Context, _ string) error { return s.err }

func newTestRouter(h *Handlers) *gin.Engine {
	gin.SetMode(gin.TestMode)
	health := &Health{}
	return NewRouter(h, health, func(c *gin.Context) { c.Status(http.StatusOK) })
}

// tests ---------------------------------------------------------------------

func TestCreateHandler_201(t *testing.T) {
	h := &Handlers{Create: &stubCreate{}}
	r := newTestRouter(h)

	body := `{"channel":"sms","recipient":"+1","content":"hi","priority":"high"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["id"] != "n1" {
		t.Fatalf("want id n1, got %v", resp["id"])
	}
}

func TestCreateHandler_ReadsIdempotencyHeader(t *testing.T) {
	create := &stubCreate{}
	r := newTestRouter(&Handlers{Create: create})

	body := `{"channel":"sms","recipient":"+1","content":"hi"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(IdempotencyHeader, "key-123")
	r.ServeHTTP(w, req)

	if create.gotKey != "key-123" {
		t.Fatalf("idempotency key not propagated, got %q", create.gotKey)
	}
}

func TestCreateHandler_ValidationError400(t *testing.T) {
	r := newTestRouter(&Handlers{Create: &stubCreate{}})

	// missing required recipient
	body := `{"channel":"sms","content":"hi"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

func TestGetHandler_NotFound404(t *testing.T) {
	r := newTestRouter(&Handlers{Get: &stubGet{err: domain.ErrNotFound}})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/notifications/missing", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
}

func TestCancelHandler_Conflict409(t *testing.T) {
	r := newTestRouter(&Handlers{Cancel: &stubCancel{err: domain.ErrNotCancellable}})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodDelete, "/api/v1/notifications/n1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d", w.Code)
	}
}

func TestCancelHandler_NoContent204(t *testing.T) {
	r := newTestRouter(&Handlers{Cancel: &stubCancel{}})

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodDelete, "/api/v1/notifications/n1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", w.Code)
	}
}
