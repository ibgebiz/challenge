// Package httpapi exposes the REST API over Gin.
package httpapi

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/ibrahim-bg/notifier/internal/infrastructure/observability"
)

// CorrelationHeader is the header carrying the request correlation id.
const CorrelationHeader = "X-Correlation-ID"

// CorrelationID middleware ensures every request has a correlation id, echoes it
// on the response, and stores it on the request context for downstream logging.
func CorrelationID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(CorrelationHeader)
		if id == "" {
			id = uuid.NewString()
		}
		c.Header(CorrelationHeader, id)
		c.Request = c.Request.WithContext(observability.WithCorrelationID(c.Request.Context(), id))
		c.Next()
	}
}
