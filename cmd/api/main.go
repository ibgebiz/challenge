// Command api serves the notification REST API, WebSocket stream, metrics, and
// Swagger UI.
//
//	@title			Notification System API
//	@version		1.0
//	@description	Event-driven notification system: create, query, cancel, and
//	@description	stream notifications across SMS, Email, and Push channels.
//	@BasePath		/
package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	_ "github.com/ibrahim-bg/notifier/api/openapi" // register generated swagger spec
	httpapi "github.com/ibrahim-bg/notifier/internal/adapter/http"
	"github.com/ibrahim-bg/notifier/internal/adapter/ws"
	"github.com/ibrahim-bg/notifier/internal/app"
	"github.com/ibrahim-bg/notifier/internal/usecase"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := app.New(rootCtx, "api")
	if err != nil {
		panic(err)
	}
	defer c.Close(context.Background())

	idGen := uuid.NewString
	clock := app.RealClock{}

	create := &usecase.CreateNotification{
		Repo: c.NotifRepo, Queue: c.Queue, Sched: c.Scheduled, Idem: c.Idem,
		Templates: c.TemplateRepo, Clock: clock, IDGen: idGen,
	}
	handlers := &httpapi.Handlers{
		Create:         create,
		CreateBatchSvc: &usecase.CreateBatch{Create: create, Batches: c.BatchRepo, Clock: clock, IDGen: idGen},
		Get:            &usecase.GetNotification{Repo: c.NotifRepo},
		List:           &usecase.ListNotifications{Repo: c.NotifRepo},
		Cancel:         &usecase.CancelNotification{Repo: c.NotifRepo, Queue: c.Queue, Sched: c.Scheduled, Clock: clock},
		BatchStatus:    &usecase.GetBatchStatus{Batches: c.BatchRepo},
		CreateTemplate: &usecase.CreateTemplate{Repo: c.TemplateRepo, Clock: clock, IDGen: idGen},
		ListTemplates:  &usecase.ListTemplates{Repo: c.TemplateRepo},
	}

	health := &httpapi.Health{
		DB:    pgPinger{c.Pool},
		Redis: redisPinger{c.Redis},
	}
	promHandler := gin.WrapH(promhttp.HandlerFor(c.Metrics.Registry(), promhttp.HandlerOpts{}))

	router := httpapi.NewRouter(handlers, health, promHandler)

	// WebSocket status stream and its Redis bridge (worker events -> local hub).
	hub := ws.NewHub()
	go ws.BridgeToHub(rootCtx, c.Redis, hub, c.Logger)
	router.GET("/ws/notifications", ws.NewHandler(hub).Stream)

	// Keep system-wide queue-depth and DLQ gauges fresh from shared Redis state.
	go c.Metrics.RunGaugeUpdater(rootCtx, 5*time.Second, c.Queue.Depth, c.DLQ.Size)

	srv := &http.Server{
		Addr:              ":" + c.Cfg.HTTPPort,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		c.Logger.Info("api listening", "port", c.Cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.Logger.Error("server error", "error", err)
			stop()
		}
	}()

	<-rootCtx.Done()
	c.Logger.Info("shutting down api")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

type pgPinger struct{ pool *pgxpool.Pool }

func (p pgPinger) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

type redisPinger struct{ rdb *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error { return p.rdb.Ping(ctx).Err() }
