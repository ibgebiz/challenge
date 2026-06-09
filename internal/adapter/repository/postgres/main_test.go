//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	notifier "github.com/ibrahim-bg/notifier"
	"github.com/ibrahim-bg/notifier/internal/infrastructure/db"
)

func newUUID() string { return uuid.NewString() }

// newTestPool spins up an ephemeral Postgres container, runs migrations, and
// returns a connected pool. The container is terminated on test cleanup.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(
		ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("notifier"),
		tcpostgres.WithUsername("notifier"),
		tcpostgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	if err := db.Migrate(dsn, notifier.MigrationsFS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := db.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
