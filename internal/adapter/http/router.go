package httpapi

import (
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// NewRouter wires all routes: the v1 API, health checks, metrics, and Swagger UI.
// promHandler exposes the Prometheus registry.
func NewRouter(h *Handlers, health *Health, promHandler gin.HandlerFunc) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("notifier-api"), CorrelationID())

	v1 := r.Group("/api/v1")
	{
		v1.POST("/notifications", h.CreateNotification)
		v1.POST("/notifications/batch", h.CreateBatch)
		v1.GET("/notifications", h.ListNotifications)
		v1.GET("/notifications/:id", h.GetNotification)
		v1.DELETE("/notifications/:id", h.CancelNotification)
		v1.GET("/batches/:id", h.GetBatch)
		v1.POST("/templates", h.CreateTemplateHandler)
		v1.GET("/templates", h.ListTemplatesHandler)
	}

	r.GET("/healthz", health.Live)
	r.GET("/readyz", health.Ready)
	r.GET("/metrics", promHandler)
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	return r
}
