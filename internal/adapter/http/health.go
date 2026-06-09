package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Pinger checks the health of a backing dependency.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Health serves liveness and readiness checks.
type Health struct {
	DB    Pinger
	Redis Pinger
}

// Live always reports OK; the process is running.
func (h *Health) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Ready reports OK only when Postgres and Redis are reachable.
func (h *Health) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := h.DB.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "dependency": "postgres", "error": err.Error()})
		return
	}
	if err := h.Redis.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready", "dependency": "redis", "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
