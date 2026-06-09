package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCorrelationMiddleware_SetsHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CorrelationID())
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/x", nil)
	r.ServeHTTP(w, req)

	if w.Header().Get("X-Correlation-ID") == "" {
		t.Fatal("expected correlation id header")
	}
}

func TestCorrelationMiddleware_PreservesIncoming(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CorrelationID())
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Correlation-ID", "abc-123")
	r.ServeHTTP(w, req)

	if got := w.Header().Get("X-Correlation-ID"); got != "abc-123" {
		t.Fatalf("want abc-123, got %q", got)
	}
}
