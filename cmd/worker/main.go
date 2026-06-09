// Command worker consumes the priority queue and delivers notifications,
// applying retry/DLQ logic and promoting due retries back into the queue.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ibrahim-bg/notifier/internal/adapter/provider/webhook"
	"github.com/ibrahim-bg/notifier/internal/adapter/ws"
	"github.com/ibrahim-bg/notifier/internal/app"
	"github.com/ibrahim-bg/notifier/internal/scheduler"
	"github.com/ibrahim-bg/notifier/internal/usecase"
	"github.com/ibrahim-bg/notifier/internal/worker"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := app.New(rootCtx, "worker")
	if err != nil {
		panic(err)
	}
	defer c.Close(context.Background())

	provider := webhook.New(c.Cfg.ProviderURL, &http.Client{Timeout: 10 * time.Second})

	process := &usecase.ProcessNotification{
		Repo: c.NotifRepo, Provider: provider, RateLimiter: c.RateLimiter,
		Attempts: c.AttemptRepo, Retry: c.Retry, DLQ: c.DLQ, Queue: c.Queue,
		Publisher: ws.NewRedisPublisher(c.Redis), Metrics: c.Metrics, Clock: app.RealClock{},
		MaxAttempts: c.Cfg.MaxRetryAttempts, RetryInterval: c.Cfg.RetryInterval,
		IDGen: uuid.NewString,
	}

	// Expose worker delivery metrics for Prometheus to scrape.
	go serveMetrics(c.Metrics.Registry(), c.Logger)

	// Promote due retries back into the queue on a short interval.
	go scheduler.Run(rootCtx, scheduler.QueuePromoter{Src: c.Retry, Dst: c.Queue}, time.Second)

	// Wrap processing in a trace span so worker delivery shows up in Jaeger.
	tracer := otel.Tracer("notifier-worker")
	processFn := func(ctx context.Context, item usecase.QueueItem) error {
		ctx, span := tracer.Start(ctx, "process_notification",
			trace.WithAttributes(attribute.String("notification.id", item.NotificationID)))
		defer span.End()
		return process.Execute(ctx, item)
	}

	c.Logger.Info("worker started", "concurrency", c.Cfg.WorkerConcurrency)
	worker.Run(rootCtx, c.Queue, processFn, c.Cfg.WorkerConcurrency)
	c.Logger.Info("worker stopped")
}

// WorkerMetricsPort is the port the worker exposes /metrics on.
const WorkerMetricsPort = "8081"

func serveMetrics(reg *prometheus.Registry, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: ":" + WorkerMetricsPort, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("metrics server error", "error", err)
	}
}
