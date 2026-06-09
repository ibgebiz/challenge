// Package app is the shared composition root. It builds infrastructure and
// adapters once; each binary (api, worker, scheduler) wires the interactors and
// entrypoints it needs from the resulting Container.
package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	notifier "github.com/ibrahim-bg/notifier"
	redisidem "github.com/ibrahim-bg/notifier/internal/adapter/idempotency/redis"
	redisqueue "github.com/ibrahim-bg/notifier/internal/adapter/queue/redis"
	redisratelimit "github.com/ibrahim-bg/notifier/internal/adapter/ratelimit/redis"
	"github.com/ibrahim-bg/notifier/internal/adapter/repository/postgres"
	"github.com/ibrahim-bg/notifier/internal/infrastructure/config"
	"github.com/ibrahim-bg/notifier/internal/infrastructure/db"
	"github.com/ibrahim-bg/notifier/internal/infrastructure/observability"
	"github.com/ibrahim-bg/notifier/internal/infrastructure/redisclient"
)

// Container holds shared infrastructure, adapters, and repositories.
type Container struct {
	Cfg    config.Config
	Logger *slog.Logger

	Pool    *pgxpool.Pool
	Redis   *redis.Client
	Metrics *observability.Metrics

	NotifRepo    *postgres.NotificationRepo
	TemplateRepo *postgres.TemplateRepo
	BatchRepo    *postgres.BatchRepo
	AttemptRepo  *postgres.AttemptRepo

	Queue       *redisqueue.Queue
	Retry       *redisqueue.Retry
	DLQ         *redisqueue.DLQ
	Scheduled   *redisqueue.Scheduled
	RateLimiter *redisratelimit.Limiter
	Idem        *redisidem.Store

	tracingShutdown observability.ShutdownFunc
}

// New loads config, connects Postgres and Redis, runs migrations, initializes
// observability, and constructs all adapters. serviceName labels traces.
func New(ctx context.Context, serviceName string) (*Container, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	logger := observability.NewLogger(cfg.LogLevel)

	shutdown, err := observability.InitTracing(ctx, serviceName, cfg.OTELEndpoint)
	if err != nil {
		return nil, err
	}

	if err := db.Migrate(cfg.PostgresDSN, notifier.MigrationsFS); err != nil {
		return nil, err
	}
	pool, err := db.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	rdb := redisclient.New(cfg.RedisAddr)

	c := &Container{
		Cfg:             cfg,
		Logger:          logger,
		Pool:            pool,
		Redis:           rdb,
		Metrics:         observability.NewMetrics(),
		NotifRepo:       postgres.NewNotificationRepo(pool),
		TemplateRepo:    postgres.NewTemplateRepo(pool),
		BatchRepo:       postgres.NewBatchRepo(pool),
		AttemptRepo:     postgres.NewAttemptRepo(pool),
		Queue:           redisqueue.New(rdb),
		Retry:           redisqueue.NewRetry(rdb),
		DLQ:             redisqueue.NewDLQ(rdb),
		Scheduled:       redisqueue.NewScheduled(rdb),
		RateLimiter:     redisratelimit.New(rdb, cfg.RateLimitPerSec),
		Idem:            redisidem.New(rdb, 24*time.Hour),
		tracingShutdown: shutdown,
	}
	return c, nil
}

// Close releases all resources. It is safe to call once during shutdown.
func (c *Container) Close(ctx context.Context) {
	if c.tracingShutdown != nil {
		_ = c.tracingShutdown(ctx)
	}
	if c.Pool != nil {
		c.Pool.Close()
	}
	if c.Redis != nil {
		_ = c.Redis.Close()
	}
}
