# Event-Driven Notification System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a scalable event-driven notification system in Go that accepts requests via REST, processes them asynchronously through Redis-backed priority queues, delivers to an external provider (webhook.site), and exposes real-time status + full observability.

**Architecture:** Clean Architecture — `domain` (entities), `usecase` (interactors + ports), `adapter` (port implementations: http/postgres/redis/provider/ws), `infrastructure` (db, redis, config, observability). Three runnable binaries (`cmd/api`, `cmd/worker`, `cmd/scheduler`) share `internal/`; they communicate only through Postgres (state) and Redis (transport).

**Tech Stack:** Go 1.2x, Gin, PostgreSQL (pgx), Redis (go-redis), golang-migrate, Prometheus, Grafana, OpenTelemetry→Jaeger, slog, swaggo, golangci-lint, testcontainers-go.

**Spec:** `docs/superpowers/specs/2026-06-09-notification-system-design.md`

**Conventions for every task:** TDD where logic exists (failing test → run → implement → run → commit). Keep files focused and small. Commit after each task. Run `make lint` before committing once the Makefile exists.

---

## Phase 0 — Project scaffolding

### Task 0.1: Initialize module and tooling

**Files:**
- Create: `go.mod`, `Makefile`, `.golangci.yml`, `.gitignore` (already exists — extend), `.env.example`

- [ ] **Step 1: Init module**

Run:
```bash
go mod init github.com/ibrahim-bg/notifier
```

- [ ] **Step 2: Create `.golangci.yml`**

```yaml
run:
  timeout: 5m
linters:
  enable:
    - govet
    - staticcheck
    - errcheck
    - revive
    - gocritic
    - gosec
    - gofumpt
    - ineffassign
    - unconvert
    - misspell
issues:
  exclude-rules:
    - path: _test\.go
      linters: [gosec, errcheck]
```

- [ ] **Step 3: Create `Makefile`**

```makefile
.PHONY: fmt lint test test-unit test-integration build up down migrate swagger

fmt:
	gofumpt -w .
	goimports -w .

lint:
	golangci-lint run ./...

test:
	go test ./... -count=1

test-unit:
	go test ./internal/domain/... ./internal/usecase/... -count=1

test-integration:
	go test ./internal/adapter/... -count=1 -tags=integration

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/scheduler ./cmd/scheduler

up:
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

swagger:
	swag init -g cmd/api/main.go -o api/openapi
```

- [ ] **Step 4: Create `.env.example`**

```bash
HTTP_PORT=8080
POSTGRES_DSN=postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable
REDIS_ADDR=localhost:6379
RATE_LIMIT_PER_SEC=100
MAX_RETRY_ATTEMPTS=5
RETRY_INTERVAL=30s
SCHEDULER_POLL_INTERVAL=5s
WORKER_CONCURRENCY=10
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
LOG_LEVEL=info
```

- [ ] **Step 5: Extend `.gitignore`**

Append:
```
/bin/
.env
```

- [ ] **Step 6: Commit**

```bash
git add go.mod Makefile .golangci.yml .gitignore .env.example
git commit -m "chore: scaffold module, makefile, linting"
```

### Task 0.2: Config loader

**Files:**
- Create: `internal/infrastructure/config/config.go`
- Test: `internal/infrastructure/config/config_test.go`

- [ ] **Step 1: Write failing test**

```go
package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "dsn")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RateLimitPerSec != 100 {
		t.Fatalf("want 100, got %d", cfg.RateLimitPerSec)
	}
	if cfg.MaxRetryAttempts != 5 {
		t.Fatalf("want 5, got %d", cfg.MaxRetryAttempts)
	}
	if cfg.RetryInterval != 30*time.Second {
		t.Fatalf("want 30s, got %v", cfg.RetryInterval)
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/infrastructure/config/...`
Expected: FAIL (package/Load undefined).

- [ ] **Step 3: Implement**

```go
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPPort              string
	PostgresDSN           string
	RedisAddr             string
	RateLimitPerSec       int
	MaxRetryAttempts      int
	RetryInterval         time.Duration
	SchedulerPollInterval time.Duration
	WorkerConcurrency     int
	OTELEndpoint          string
	LogLevel              string
}

func Load() (Config, error) {
	c := Config{
		HTTPPort:              getEnv("HTTP_PORT", "8080"),
		PostgresDSN:           os.Getenv("POSTGRES_DSN"),
		RedisAddr:             os.Getenv("REDIS_ADDR"),
		RateLimitPerSec:       getInt("RATE_LIMIT_PER_SEC", 100),
		MaxRetryAttempts:      getInt("MAX_RETRY_ATTEMPTS", 5),
		RetryInterval:         getDur("RETRY_INTERVAL", 30*time.Second),
		SchedulerPollInterval: getDur("SCHEDULER_POLL_INTERVAL", 5*time.Second),
		WorkerConcurrency:     getInt("WORKER_CONCURRENCY", 10),
		OTELEndpoint:          os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
	}
	if c.PostgresDSN == "" {
		return c, fmt.Errorf("POSTGRES_DSN required")
	}
	if c.RedisAddr == "" {
		return c, fmt.Errorf("REDIS_ADDR required")
	}
	return c, nil
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/infrastructure/config/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/infrastructure/config
git commit -m "feat: env-based config loader"
```

---

## Phase 1 — Domain layer

### Task 1.1: Enums and entities

**Files:**
- Create: `internal/domain/notification.go`, `internal/domain/enums.go`, `internal/domain/errors.go`
- Test: `internal/domain/notification_test.go`

- [ ] **Step 1: Write failing test**

```go
package domain

import "testing"

func TestChannelValid(t *testing.T) {
	if !ChannelSMS.Valid() {
		t.Fatal("sms should be valid")
	}
	if Channel("fax").Valid() {
		t.Fatal("fax should be invalid")
	}
}

func TestStatusCancellable(t *testing.T) {
	cases := map[Status]bool{
		StatusPending:   true,
		StatusQueued:    true,
		StatusSending:   false,
		StatusDelivered: false,
		StatusFailed:    false,
		StatusCancelled: false,
	}
	for s, want := range cases {
		if s.Cancellable() != want {
			t.Fatalf("%s cancellable=%v want %v", s, s.Cancellable(), want)
		}
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/domain/...`
Expected: FAIL (undefined).

- [ ] **Step 3: Implement `enums.go`**

```go
package domain

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

type Status string

const (
	StatusPending   Status = "pending"
	StatusQueued    Status = "queued"
	StatusSending   Status = "sending"
	StatusDelivered Status = "delivered"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func (s Status) Cancellable() bool {
	return s == StatusPending || s == StatusQueued
}
```

- [ ] **Step 4: Implement `errors.go`**

```go
package domain

import "errors"

var (
	ErrValidation   = errors.New("validation failed")
	ErrNotFound     = errors.New("not found")
	ErrNotCancellable = errors.New("notification is not cancellable")
	ErrDuplicate    = errors.New("duplicate idempotency key")
)
```

- [ ] **Step 5: Implement `notification.go`**

```go
package domain

import "time"

type Notification struct {
	ID                string
	BatchID           *string
	Channel           Channel
	Recipient         string
	Content           string
	TemplateID        *string
	Variables         map[string]string
	Priority          Priority
	Status            Status
	IdempotencyKey    *string
	ScheduledAt       *time.Time
	Attempts          int
	LastError         *string
	ProviderMessageID *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Batch struct {
	ID        string
	Total     int
	CreatedAt time.Time
}

type DeliveryAttempt struct {
	ID               string
	NotificationID   string
	AttemptNo        int
	Status           string
	ProviderResponse map[string]any
	Error            *string
	AttemptedAt      time.Time
}

type Template struct {
	ID        string
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
}
```

- [ ] **Step 6: Run to verify pass**

Run: `go test ./internal/domain/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/domain && git commit -m "feat: domain entities and enums"
```

### Task 1.2: Content validation

**Files:**
- Create: `internal/domain/validation.go`
- Test: `internal/domain/validation_test.go`

- [ ] **Step 1: Write failing test**

```go
package domain

import "testing"

func TestValidateNotification(t *testing.T) {
	valid := Notification{Channel: ChannelالسMS, Recipient: "+905551234567", Content: "hi", Priority: PriorityNormal}
	if err := ValidateNotification(valid); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
	tooLong := Notification{Channel: ChannelSMS, Recipient: "+905551234567", Content: string(make([]byte, 1601)), Priority: PriorityNormal}
	if err := ValidateNotification(tooLong); err == nil {
		t.Fatal("expected sms length error")
	}
	bad := Notification{Channel: "fax", Recipient: "x", Content: "hi", Priority: PriorityNormal}
	if err := ValidateNotification(bad); err == nil {
		t.Fatal("expected channel error")
	}
}
```

> Note: fix the typo `ChannelالسMS` → `ChannelSMS` when writing the real test (placeholder shows intent: a valid SMS notification).

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/domain/... -run TestValidateNotification`
Expected: FAIL.

- [ ] **Step 3: Implement**

```go
package domain

import "fmt"

// per-channel content character limits
var channelMaxLen = map[Channel]int{
	ChannelSMS:   1600,
	ChannelEmail: 100000,
	ChannelPush:  4000,
}

func ValidateNotification(n Notification) error {
	if !n.Channel.Valid() {
		return fmt.Errorf("%w: invalid channel %q", ErrValidation, n.Channel)
	}
	if !n.Priority.Valid() {
		return fmt.Errorf("%w: invalid priority %q", ErrValidation, n.Priority)
	}
	if n.Recipient == "" {
		return fmt.Errorf("%w: recipient required", ErrValidation)
	}
	// content may be empty only when a template will render it
	if n.Content == "" && n.TemplateID == nil {
		return fmt.Errorf("%w: content or template required", ErrValidation)
	}
	if max := channelMaxLen[n.Channel]; len(n.Content) > max {
		return fmt.Errorf("%w: content exceeds %d chars for %s", ErrValidation, max, n.Channel)
	}
	return nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/domain/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/domain && git commit -m "feat: notification content validation"
```

### Task 1.3: Template rendering

**Files:**
- Create: `internal/domain/template.go` (extend), `internal/usecase/template_render.go` is later — keep pure render in domain.
- Add render to `internal/domain/template.go`
- Test: `internal/domain/template_test.go`

- [ ] **Step 1: Write failing test**

```go
package domain

import "testing"

func TestRender(t *testing.T) {
	out, err := Render("Hello {{name}}, code {{code}}", map[string]string{"name": "Ada", "code": "42"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "Hello Ada, code 42" {
		t.Fatalf("got %q", out)
	}
	if _, err := Render("Hi {{missing}}", map[string]string{}); err == nil {
		t.Fatal("expected missing-var error")
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/domain/... -run TestRender`
Expected: FAIL.

- [ ] **Step 3: Implement (append to `template.go`)**

```go
import (
	"fmt"
	"regexp"
)

var tmplVar = regexp.MustCompile(`{{\s*(\w+)\s*}}`)

func Render(body string, vars map[string]string) (string, error) {
	var missing string
	out := tmplVar.ReplaceAllStringFunc(body, func(m string) string {
		key := tmplVar.FindStringSubmatch(m)[1]
		v, ok := vars[key]
		if !ok {
			missing = key
			return m
		}
		return v
	})
	if missing != "" {
		return "", fmt.Errorf("%w: missing template variable %q", ErrValidation, missing)
	}
	return out, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/domain/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/domain && git commit -m "feat: template variable rendering"
```

---

## Phase 2 — Usecase ports & interactors

### Task 2.1: Define ports

**Files:**
- Create: `internal/usecase/ports.go`

- [ ] **Step 1: Implement ports (interfaces only — no test needed)**

```go
package usecase

import (
	"context"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type NotificationRepository interface {
	Create(ctx context.Context, n domain.Notification) error
	CreateBatch(ctx context.Context, b domain.Batch, ns []domain.Notification) error
	Get(ctx context.Context, id string) (domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error)
	UpdateStatus(ctx context.Context, id string, status domain.Status, lastErr *string, providerMsgID *string, attempts int) error
	List(ctx context.Context, f NotificationFilter) ([]domain.Notification, int, error)
}

type BatchRepository interface {
	Get(ctx context.Context, id string) (domain.Batch, error)
	StatusCounts(ctx context.Context, batchID string) (map[domain.Status]int, error)
}

type TemplateRepository interface {
	Create(ctx context.Context, t domain.Template) error
	Get(ctx context.Context, id string) (domain.Template, error)
	List(ctx context.Context) ([]domain.Template, error)
}

type DeliveryAttemptRepository interface {
	Add(ctx context.Context, a domain.DeliveryAttempt) error
}

type QueueItem struct {
	NotificationID string
	Priority       domain.Priority
}

type Queue interface {
	Enqueue(ctx context.Context, item QueueItem) error
	// Dequeue blocks up to timeout; returns ok=false on timeout.
	Dequeue(ctx context.Context, timeout time.Duration) (QueueItem, bool, error)
	Remove(ctx context.Context, notificationID string) error
	Depth(ctx context.Context, p domain.Priority) (int64, error)
}

type RetryQueue interface {
	Schedule(ctx context.Context, item QueueItem, at time.Time) error
	DuePromote(ctx context.Context, now time.Time, dst Queue) (int, error)
	Size(ctx context.Context) (int64, error)
}

type DLQ interface {
	Push(ctx context.Context, notificationID string, reason string) error
	Size(ctx context.Context) (int64, error)
}

type ScheduledStore interface {
	Add(ctx context.Context, item QueueItem, at time.Time) error
	Remove(ctx context.Context, notificationID string) error
	DuePromote(ctx context.Context, now time.Time, dst Queue) (int, error)
}

type RateLimiter interface {
	// Allow reports whether a token was available for the channel right now.
	Allow(ctx context.Context, channel domain.Channel) (bool, error)
}

type IdempotencyStore interface {
	// Remember marks key seen with the notification id; returns existing id if already present.
	Remember(ctx context.Context, key, notificationID string) (existing string, found bool, err error)
}

type ProviderResponse struct {
	MessageID string
	Status    string
	Timestamp string
}

type Provider interface {
	Send(ctx context.Context, n domain.Notification) (ProviderResponse, error)
}

type StatusEvent struct {
	NotificationID string
	BatchID        *string
	Status         domain.Status
	At             time.Time
}

type EventPublisher interface {
	Publish(ctx context.Context, e StatusEvent)
}

type Clock interface{ Now() time.Time }

type NotificationFilter struct {
	Status    *domain.Status
	Channel   *domain.Channel
	From      *time.Time
	To        *time.Time
	Limit     int
	Offset    int
}
```

- [ ] **Step 2: Verify compiles**

Run: `go build ./internal/usecase/...`
Expected: builds (no impls yet, interfaces only).

- [ ] **Step 3: Commit**

```bash
git add internal/usecase && git commit -m "feat: usecase ports"
```

### Task 2.2: Fakes for testing

**Files:**
- Create: `internal/usecase/fakes_test.go`

- [ ] **Step 1: Implement in-memory fakes for every port**

Provide minimal map-backed fakes: `fakeNotifRepo`, `fakeTemplateRepo`, `fakeQueue`, `fakeIdem`, `fakeRateLimiter`, `fakeProvider`, `fakePublisher`, `fixedClock`. Each implements its port using a `sync.Mutex` + map/slice. Example for the repo and clock:

```go
package usecase

import (
	"context"
	"sync"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type fixedClock struct{ t time.Time }
func (c fixedClock) Now() time.Time { return c.t }

type fakeNotifRepo struct {
	mu    sync.Mutex
	items map[string]domain.Notification
	byKey map[string]string
}
func newFakeRepo() *fakeNotifRepo {
	return &fakeNotifRepo{items: map[string]domain.Notification{}, byKey: map[string]string{}}
}
func (r *fakeNotifRepo) Create(_ context.Context, n domain.Notification) error {
	r.mu.Lock(); defer r.mu.Unlock()
	r.items[n.ID] = n
	if n.IdempotencyKey != nil { r.byKey[*n.IdempotencyKey] = n.ID }
	return nil
}
func (r *fakeNotifRepo) CreateBatch(ctx context.Context, _ domain.Batch, ns []domain.Notification) error {
	for _, n := range ns { _ = r.Create(ctx, n) }
	return nil
}
func (r *fakeNotifRepo) Get(_ context.Context, id string) (domain.Notification, error) {
	r.mu.Lock(); defer r.mu.Unlock()
	n, ok := r.items[id]
	if !ok { return domain.Notification{}, domain.ErrNotFound }
	return n, nil
}
func (r *fakeNotifRepo) GetByIdempotencyKey(_ context.Context, key string) (domain.Notification, error) {
	r.mu.Lock(); defer r.mu.Unlock()
	id, ok := r.byKey[key]
	if !ok { return domain.Notification{}, domain.ErrNotFound }
	return r.items[id], nil
}
func (r *fakeNotifRepo) UpdateStatus(_ context.Context, id string, s domain.Status, le *string, pmid *string, attempts int) error {
	r.mu.Lock(); defer r.mu.Unlock()
	n := r.items[id]; n.Status = s; n.LastError = le; n.ProviderMessageID = pmid; n.Attempts = attempts
	r.items[id] = n
	return nil
}
func (r *fakeNotifRepo) List(_ context.Context, _ NotificationFilter) ([]domain.Notification, int, error) {
	r.mu.Lock(); defer r.mu.Unlock()
	out := make([]domain.Notification, 0, len(r.items))
	for _, n := range r.items { out = append(out, n) }
	return out, len(out), nil
}
```

Implement the remaining fakes following the same pattern (queue as a slice keyed by priority; idempotency as a map with first-writer-wins; rate limiter returns a configurable bool; provider returns a canned `ProviderResponse` or error via a func field; publisher appends events to a slice).

- [ ] **Step 2: Verify compiles**

Run: `go vet ./internal/usecase/...`
Expected: builds.

- [ ] **Step 3: Commit**

```bash
git add internal/usecase/fakes_test.go && git commit -m "test: usecase port fakes"
```

### Task 2.3: CreateNotification interactor

**Files:**
- Create: `internal/usecase/create_notification.go`
- Test: `internal/usecase/create_notification_test.go`

- [ ] **Step 1: Write failing test**

```go
package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func newCreateSvc(repo *fakeNotifRepo, q *fakeQueue, idem *fakeIdem, tmpl *fakeTemplateRepo) *CreateNotification {
	return &CreateNotification{
		Repo: repo, Queue: q, Idem: idem, Templates: tmpl,
		Sched: newFakeScheduled(), Clock: fixedClock{t: time.Unix(1000, 0)},
		IDGen: func() string { return "fixed-id" },
	}
}

func TestCreate_EnqueuesImmediate(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	out, err := svc.Execute(context.Background(), CreateInput{
		Channel: domain.ChannelSMS, Recipient: "+90555", Content: "hi", Priority: domain.PriorityHigh,
	})
	if err != nil { t.Fatalf("err: %v", err) }
	if out.Status != domain.StatusQueued { t.Fatalf("want queued, got %s", out.Status) }
	if got, _ := q.len(domain.PriorityHigh); got != 1 { t.Fatalf("want 1 queued, got %d", got) }
}

func TestCreate_IdempotentReturnsExisting(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	key := "k1"
	in := CreateInput{Channel: domain.ChannelSMS, Recipient: "+90555", Content: "hi", Priority: domain.PriorityNormal, IdempotencyKey: &key}
	first, _ := svc.Execute(context.Background(), in)
	second, err := svc.Execute(context.Background(), in)
	if err != nil { t.Fatalf("err: %v", err) }
	if first.ID != second.ID { t.Fatalf("ids differ: %s vs %s", first.ID, second.ID) }
	if got, _ := q.len(domain.PriorityNormal); got != 1 { t.Fatalf("want 1 enqueue, got %d", got) }
}

func TestCreate_ScheduledNotEnqueued(t *testing.T) {
	repo, q, idem, tmpl := newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()
	svc := newCreateSvc(repo, q, idem, tmpl)
	future := time.Unix(2000, 0)
	out, err := svc.Execute(context.Background(), CreateInput{
		Channel: domain.ChannelSMS, Recipient: "+90555", Content: "hi", Priority: domain.PriorityLow, ScheduledAt: &future,
	})
	if err != nil { t.Fatalf("err: %v", err) }
	if out.Status != domain.StatusPending { t.Fatalf("want pending, got %s", out.Status) }
	if got, _ := q.len(domain.PriorityLow); got != 0 { t.Fatalf("want 0 enqueue, got %d", got) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/usecase/... -run TestCreate`
Expected: FAIL (CreateNotification/CreateInput undefined). Add `newFakeScheduled`, `fakeScheduled`, and `q.len` helper to fakes if missing.

- [ ] **Step 3: Implement**

```go
package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type CreateInput struct {
	Channel        domain.Channel
	Recipient      string
	Content        string
	TemplateID     *string
	Variables      map[string]string
	Priority       domain.Priority
	IdempotencyKey *string
	ScheduledAt    *time.Time
	BatchID        *string
}

type CreateNotification struct {
	Repo      NotificationRepository
	Queue     Queue
	Sched     ScheduledStore
	Idem      IdempotencyStore
	Templates TemplateRepository
	Clock     Clock
	IDGen     func() string
}

func (s *CreateNotification) Execute(ctx context.Context, in CreateInput) (domain.Notification, error) {
	if in.Priority == "" {
		in.Priority = domain.PriorityNormal
	}

	// idempotency pre-check
	if in.IdempotencyKey != nil {
		if existing, err := s.Repo.GetByIdempotencyKey(ctx, *in.IdempotencyKey); err == nil {
			return existing, nil
		} else if !errors.Is(err, domain.ErrNotFound) {
			return domain.Notification{}, err
		}
	}

	content := in.Content
	if in.TemplateID != nil {
		tmpl, err := s.Templates.Get(ctx, *in.TemplateID)
		if err != nil {
			return domain.Notification{}, err
		}
		rendered, err := domain.Render(tmpl.Body, in.Variables)
		if err != nil {
			return domain.Notification{}, err
		}
		content = rendered
	}

	now := s.Clock.Now()
	n := domain.Notification{
		ID:             s.IDGen(),
		BatchID:        in.BatchID,
		Channel:        in.Channel,
		Recipient:      in.Recipient,
		Content:        content,
		TemplateID:     in.TemplateID,
		Variables:      in.Variables,
		Priority:       in.Priority,
		Status:         domain.StatusPending,
		IdempotencyKey: in.IdempotencyKey,
		ScheduledAt:    in.ScheduledAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := domain.ValidateNotification(n); err != nil {
		return domain.Notification{}, err
	}

	// claim idempotency key (race-safe at store layer; DB unique is source of truth)
	if in.IdempotencyKey != nil {
		if existing, found, err := s.Idem.Remember(ctx, *in.IdempotencyKey, n.ID); err != nil {
			return domain.Notification{}, err
		} else if found {
			return s.Repo.Get(ctx, existing)
		}
	}

	if err := s.Repo.Create(ctx, n); err != nil {
		return domain.Notification{}, err
	}

	item := QueueItem{NotificationID: n.ID, Priority: n.Priority}
	if in.ScheduledAt != nil && in.ScheduledAt.After(now) {
		if err := s.Sched.Add(ctx, item, *in.ScheduledAt); err != nil {
			return domain.Notification{}, err
		}
		return n, nil // remains pending
	}

	if err := s.Queue.Enqueue(ctx, item); err != nil {
		return domain.Notification{}, err
	}
	n.Status = domain.StatusQueued
	if err := s.Repo.UpdateStatus(ctx, n.ID, domain.StatusQueued, nil, nil, 0); err != nil {
		return domain.Notification{}, err
	}
	return n, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/usecase/... -run TestCreate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase && git commit -m "feat: CreateNotification interactor"
```

### Task 2.4: CreateBatch interactor

**Files:**
- Create: `internal/usecase/create_batch.go`
- Test: `internal/usecase/create_batch_test.go`

- [ ] **Step 1: Write failing test**

```go
package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestCreateBatch_RejectsOver1000(t *testing.T) {
	svc := &CreateBatch{Create: newCreateSvc(newFakeRepo(), newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()),
		Repo: newFakeRepo(), Clock: fixedClock{t: time.Unix(1, 0)}, IDGen: func() string { return "b" }}
	items := make([]CreateInput, 1001)
	if _, err := svc.Execute(context.Background(), items); err == nil {
		t.Fatal("expected size error")
	}
}

func TestCreateBatch_CreatesAll(t *testing.T) {
	repo := newFakeRepo()
	svc := &CreateBatch{Create: newCreateSvc(repo, newFakeQueue(), newFakeIdem(), newFakeTemplateRepo()),
		Repo: repo, Clock: fixedClock{t: time.Unix(1, 0)}, IDGen: func() string { return "b" }}
	items := []CreateInput{
		{Channel: domain.ChannelSMS, Recipient: "+1", Content: "a", Priority: domain.PriorityNormal},
		{Channel: domain.ChannelEmail, Recipient: "x@y.z", Content: "b", Priority: domain.PriorityNormal},
	}
	out, err := svc.Execute(context.Background(), items)
	if err != nil { t.Fatalf("err: %v", err) }
	if out.Total != 2 { t.Fatalf("want 2, got %d", out.Total) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/usecase/... -run TestCreateBatch`
Expected: FAIL.

- [ ] **Step 3: Implement**

```go
package usecase

import (
	"context"
	"fmt"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

const MaxBatchSize = 1000

type CreateBatch struct {
	Create *CreateNotification
	Repo   NotificationRepository
	Clock  Clock
	IDGen  func() string
}

func (s *CreateBatch) Execute(ctx context.Context, items []CreateInput) (domain.Batch, error) {
	if len(items) == 0 {
		return domain.Batch{}, fmt.Errorf("%w: batch is empty", domain.ErrValidation)
	}
	if len(items) > MaxBatchSize {
		return domain.Batch{}, fmt.Errorf("%w: batch exceeds %d", domain.ErrValidation, MaxBatchSize)
	}
	batchID := s.IDGen()
	batch := domain.Batch{ID: batchID, Total: len(items), CreatedAt: s.Clock.Now()}
	for i := range items {
		items[i].BatchID = &batchID
		if _, err := s.Create.Execute(ctx, items[i]); err != nil {
			return domain.Batch{}, fmt.Errorf("item %d: %w", i, err)
		}
	}
	return batch, nil
}
```

> Note: the fake repo's `CreateBatch` is unused here because we delegate to per-item Create for simplicity and idempotency reuse. The Postgres adapter (Task 3.x) wraps the whole batch in a transaction at the `Create.Execute` boundary via a Tx-aware repo passed in; for this interactor, correctness is per-item create + a returned Batch record. Persisting the Batch row happens in the Postgres `CreateBatch` path wired in the composition root (Task 11). For the fake test we only assert Total.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/usecase/... -run TestCreateBatch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase && git commit -m "feat: CreateBatch interactor"
```

### Task 2.5: CancelNotification interactor

**Files:**
- Create: `internal/usecase/cancel_notification.go`
- Test: `internal/usecase/cancel_notification_test.go`

- [ ] **Step 1: Write failing test**

```go
package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestCancel_QueuedSucceeds(t *testing.T) {
	repo, q := newFakeRepo(), newFakeQueue()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Status: domain.StatusQueued, Priority: domain.PriorityNormal})
	svc := &CancelNotification{Repo: repo, Queue: q, Sched: newFakeScheduled(), Clock: fixedClock{t: time.Unix(1, 0)}}
	if err := svc.Execute(context.Background(), "n1"); err != nil { t.Fatalf("err: %v", err) }
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusCancelled { t.Fatalf("want cancelled, got %s", n.Status) }
}

func TestCancel_SendingFails(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n2", Status: domain.StatusSending})
	svc := &CancelNotification{Repo: repo, Queue: newFakeQueue(), Sched: newFakeScheduled(), Clock: fixedClock{t: time.Unix(1, 0)}}
	if err := svc.Execute(context.Background(), "n2"); err == nil {
		t.Fatal("expected not-cancellable error")
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/usecase/... -run TestCancel`
Expected: FAIL.

- [ ] **Step 3: Implement**

```go
package usecase

import (
	"context"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type CancelNotification struct {
	Repo  NotificationRepository
	Queue Queue
	Sched ScheduledStore
	Clock Clock
}

func (s *CancelNotification) Execute(ctx context.Context, id string) error {
	n, err := s.Repo.Get(ctx, id)
	if err != nil {
		return err
	}
	if !n.Status.Cancellable() {
		return domain.ErrNotCancellable
	}
	_ = s.Queue.Remove(ctx, id)
	_ = s.Sched.Remove(ctx, id)
	return s.Repo.UpdateStatus(ctx, id, domain.StatusCancelled, nil, nil, n.Attempts)
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/usecase/... -run TestCancel`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase && git commit -m "feat: CancelNotification interactor"
```

### Task 2.6: ProcessNotification interactor (the worker core)

**Files:**
- Create: `internal/usecase/process_notification.go`
- Test: `internal/usecase/process_notification_test.go`

- [ ] **Step 1: Write failing test**

```go
package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func newProcessSvc(repo *fakeNotifRepo, prov *fakeProvider, rl *fakeRateLimiter) *ProcessNotification {
	return &ProcessNotification{
		Repo: repo, Provider: prov, RateLimiter: rl, Attempts: newFakeAttempts(),
		Retry: newFakeRetry(), DLQ: newFakeDLQ(), Queue: newFakeQueue(), Publisher: newFakePublisher(),
		Clock: fixedClock{t: time.Unix(1, 0)}, MaxAttempts: 5, RetryInterval: 30 * time.Second,
		IDGen: func() string { return "att" },
	}
}

func TestProcess_Success(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi", Status: domain.StatusQueued})
	prov := &fakeProvider{resp: ProviderResponse{MessageID: "m1", Status: "accepted"}}
	svc := newProcessSvc(repo, prov, &fakeRateLimiter{allow: true})
	if err := svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}); err != nil {
		t.Fatalf("err: %v", err)
	}
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusDelivered { t.Fatalf("want delivered, got %s", n.Status) }
	if n.ProviderMessageID == nil || *n.ProviderMessageID != "m1" { t.Fatal("provider id not stored") }
}

func TestProcess_RateLimitedRequeues(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Status: domain.StatusQueued})
	svc := newProcessSvc(repo, &fakeProvider{}, &fakeRateLimiter{allow: false})
	err := svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal})
	if !errors.Is(err, ErrRateLimited) { t.Fatalf("want ErrRateLimited, got %v", err) }
}

func TestProcess_FailureRetriesThenDLQ(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Channel: domain.ChannelSMS, Status: domain.StatusQueued, Attempts: 4})
	prov := &fakeProvider{err: errors.New("provider down")}
	svc := newProcessSvc(repo, prov, &fakeRateLimiter{allow: true})
	svc.MaxAttempts = 5
	_ = svc.Execute(context.Background(), QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal})
	n, _ := repo.Get(context.Background(), "n1")
	if n.Status != domain.StatusFailed { t.Fatalf("want failed (DLQ) after 5th attempt, got %s", n.Status) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/usecase/... -run TestProcess`
Expected: FAIL. Add `fakeProvider{resp,err}`, `fakeRateLimiter{allow}`, `newFakeAttempts/Retry/DLQ` if missing.

- [ ] **Step 3: Implement**

```go
package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

var ErrRateLimited = errors.New("rate limited")

type ProcessNotification struct {
	Repo          NotificationRepository
	Provider      Provider
	RateLimiter   RateLimiter
	Attempts      DeliveryAttemptRepository
	Retry         RetryQueue
	DLQ           DLQ
	Queue         Queue
	Publisher     EventPublisher
	Clock         Clock
	MaxAttempts   int
	RetryInterval time.Duration
	IDGen         func() string
}

func (s *ProcessNotification) Execute(ctx context.Context, item QueueItem) error {
	n, err := s.Repo.Get(ctx, item.NotificationID)
	if err != nil {
		return err
	}
	if n.Status == domain.StatusCancelled || n.Status == domain.StatusDelivered {
		return nil // nothing to do
	}

	allowed, err := s.RateLimiter.Allow(ctx, n.Channel)
	if err != nil {
		return err
	}
	if !allowed {
		// re-queue with a small delay so the worker stays free
		_ = s.Retry.Schedule(ctx, item, s.Clock.Now().Add(time.Second))
		return ErrRateLimited
	}

	_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusSending, n.LastError, n.ProviderMessageID, n.Attempts)
	s.publish(ctx, n, domain.StatusSending)

	attemptNo := n.Attempts + 1
	resp, sendErr := s.Provider.Send(ctx, n)

	att := domain.DeliveryAttempt{
		ID: s.IDGen(), NotificationID: n.ID, AttemptNo: attemptNo, AttemptedAt: s.Clock.Now(),
	}
	if sendErr != nil {
		msg := sendErr.Error()
		att.Status = "failed"
		att.Error = &msg
		_ = s.Attempts.Add(ctx, att)
		return s.handleFailure(ctx, n, item, attemptNo, msg)
	}

	att.Status = "delivered"
	att.ProviderResponse = map[string]any{"messageId": resp.MessageID, "status": resp.Status, "timestamp": resp.Timestamp}
	_ = s.Attempts.Add(ctx, att)
	_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusDelivered, nil, &resp.MessageID, attemptNo)
	s.publish(ctx, n, domain.StatusDelivered)
	return nil
}

func (s *ProcessNotification) handleFailure(ctx context.Context, n domain.Notification, item QueueItem, attemptNo int, reason string) error {
	if attemptNo >= s.MaxAttempts {
		_ = s.DLQ.Push(ctx, n.ID, reason)
		_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusFailed, &reason, nil, attemptNo)
		s.publish(ctx, n, domain.StatusFailed)
		return nil
	}
	_ = s.Repo.UpdateStatus(ctx, n.ID, domain.StatusQueued, &reason, nil, attemptNo)
	return s.Retry.Schedule(ctx, item, s.Clock.Now().Add(s.RetryInterval))
}

func (s *ProcessNotification) publish(ctx context.Context, n domain.Notification, st domain.Status) {
	s.Publisher.Publish(ctx, StatusEvent{NotificationID: n.ID, BatchID: n.BatchID, Status: st, At: s.Clock.Now()})
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/usecase/... -run TestProcess`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/usecase && git commit -m "feat: ProcessNotification interactor with retry/DLQ"
```

### Task 2.7: List, Get, Template, and Batch-status interactors

**Files:**
- Create: `internal/usecase/queries.go`, `internal/usecase/templates.go`
- Test: `internal/usecase/queries_test.go`

- [ ] **Step 1: Write failing test**

```go
package usecase

import (
	"context"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestListNotifications_Passthrough(t *testing.T) {
	repo := newFakeRepo()
	_ = repo.Create(context.Background(), domain.Notification{ID: "n1", Status: domain.StatusQueued})
	svc := &ListNotifications{Repo: repo}
	out, total, err := svc.Execute(context.Background(), NotificationFilter{Limit: 10})
	if err != nil { t.Fatalf("err: %v", err) }
	if total != 1 || len(out) != 1 { t.Fatalf("want 1/1 got %d/%d", total, len(out)) }
}

func TestCreateTemplate(t *testing.T) {
	repo := newFakeTemplateRepo()
	svc := &CreateTemplate{Repo: repo, Clock: fixedClock{}, IDGen: func() string { return "t1" }}
	out, err := svc.Execute(context.Background(), "welcome", domain.ChannelEmail, "Hi {{name}}")
	if err != nil { t.Fatalf("err: %v", err) }
	if out.ID != "t1" { t.Fatalf("want t1, got %s", out.ID) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/usecase/... -run 'TestList|TestCreateTemplate'`
Expected: FAIL.

- [ ] **Step 3: Implement `queries.go`**

```go
package usecase

import (
	"context"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type ListNotifications struct{ Repo NotificationRepository }

func (s *ListNotifications) Execute(ctx context.Context, f NotificationFilter) ([]domain.Notification, int, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	return s.Repo.List(ctx, f)
}

type GetNotification struct{ Repo NotificationRepository }

func (s *GetNotification) Execute(ctx context.Context, id string) (domain.Notification, error) {
	return s.Repo.Get(ctx, id)
}

type GetBatchStatus struct {
	Batches BatchRepository
}

func (s *GetBatchStatus) Execute(ctx context.Context, id string) (domain.Batch, map[domain.Status]int, error) {
	b, err := s.Batches.Get(ctx, id)
	if err != nil {
		return domain.Batch{}, nil, err
	}
	counts, err := s.Batches.StatusCounts(ctx, id)
	return b, counts, err
}
```

- [ ] **Step 4: Implement `templates.go`**

```go
package usecase

import (
	"context"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type CreateTemplate struct {
	Repo  TemplateRepository
	Clock Clock
	IDGen func() string
}

func (s *CreateTemplate) Execute(ctx context.Context, name string, ch domain.Channel, body string) (domain.Template, error) {
	if !ch.Valid() {
		return domain.Template{}, domain.ErrValidation
	}
	t := domain.Template{ID: s.IDGen(), Name: name, Channel: ch, Body: body, CreatedAt: s.Clock.Now()}
	return t, s.Repo.Create(ctx, t)
}

type ListTemplates struct{ Repo TemplateRepository }

func (s *ListTemplates) Execute(ctx context.Context) ([]domain.Template, error) {
	return s.Repo.List(ctx)
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/usecase/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/usecase && git commit -m "feat: query and template interactors"
```

---

## Phase 3 — Postgres adapters & migrations

### Task 3.1: Migrations

**Files:**
- Create: `migrations/0001_init.up.sql`, `migrations/0001_init.down.sql`

- [ ] **Step 1: Write `0001_init.up.sql`**

```sql
CREATE TYPE channel AS ENUM ('sms','email','push');
CREATE TYPE priority AS ENUM ('high','normal','low');
CREATE TYPE status AS ENUM ('pending','queued','sending','delivered','failed','cancelled');

CREATE TABLE batches (
  id uuid PRIMARY KEY,
  total int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE templates (
  id uuid PRIMARY KEY,
  name text NOT NULL,
  channel channel NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
  id uuid PRIMARY KEY,
  batch_id uuid REFERENCES batches(id),
  channel channel NOT NULL,
  recipient text NOT NULL,
  content text NOT NULL,
  template_id uuid REFERENCES templates(id),
  variables jsonb,
  priority priority NOT NULL DEFAULT 'normal',
  status status NOT NULL DEFAULT 'pending',
  idempotency_key text UNIQUE,
  scheduled_at timestamptz,
  attempts int NOT NULL DEFAULT 0,
  last_error text,
  provider_message_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_channel ON notifications(channel);
CREATE INDEX idx_notifications_created_at ON notifications(created_at);
CREATE INDEX idx_notifications_batch_id ON notifications(batch_id);

CREATE TABLE delivery_attempts (
  id uuid PRIMARY KEY,
  notification_id uuid NOT NULL REFERENCES notifications(id),
  attempt_no int NOT NULL,
  status text NOT NULL,
  provider_response jsonb,
  error text,
  attempted_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_attempts_notification ON delivery_attempts(notification_id);
```

- [ ] **Step 2: Write `0001_init.down.sql`**

```sql
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS templates;
DROP TABLE IF EXISTS batches;
DROP TYPE IF EXISTS status;
DROP TYPE IF EXISTS priority;
DROP TYPE IF EXISTS channel;
```

- [ ] **Step 3: Commit**

```bash
git add migrations && git commit -m "feat: initial schema migration"
```

### Task 3.2: Postgres connection + migrate runner

**Files:**
- Create: `internal/infrastructure/db/postgres.go`

- [ ] **Step 1: Implement (no unit test; covered by integration tests)**

```go
package db

import (
	"context"
	"embed"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// Migrate runs migrations from the given fs at root "migrations".
func Migrate(dsn string, files embed.FS) error {
	src, err := iofs.New(files, "migrations")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

var _ = postgres.Postgres{} // ensure driver import retained
```

> Note: place `//go:embed migrations/*.sql` on an `embed.FS` var in `main` packages, or create `internal/infrastructure/db/migrations_embed.go` with the embed and pass it in.

- [ ] **Step 2: Verify build**

Run: `go build ./internal/infrastructure/db/...`
Expected: builds (after `go get` the deps).

- [ ] **Step 3: Commit**

```bash
git add internal/infrastructure/db && git commit -m "feat: postgres connect + migrate runner"
```

### Task 3.3: Notification & related repositories (Postgres)

**Files:**
- Create: `internal/adapter/repository/postgres/notification_repo.go`, `template_repo.go`, `batch_repo.go`, `attempt_repo.go`
- Test: `internal/adapter/repository/postgres/notification_repo_integration_test.go` (build tag `integration`)

- [ ] **Step 1: Write failing integration test**

```go
//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func TestNotificationRepo_CreateGet(t *testing.T) {
	pool := newTestPool(t) // helper spins up testcontainers Postgres + runs migrations
	repo := NewNotificationRepo(pool)
	ctx := context.Background()
	n := domain.Notification{ID: newUUID(), Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi",
		Priority: domain.PriorityNormal, Status: domain.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := repo.Create(ctx, n); err != nil { t.Fatal(err) }
	got, err := repo.Get(ctx, n.ID)
	if err != nil { t.Fatal(err) }
	if got.Content != "hi" { t.Fatalf("got %q", got.Content) }
}

func TestNotificationRepo_IdempotencyUnique(t *testing.T) {
	pool := newTestPool(t)
	repo := NewNotificationRepo(pool)
	ctx := context.Background()
	key := "dup"
	mk := func(id string) domain.Notification {
		return domain.Notification{ID: id, Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi",
			Priority: domain.PriorityNormal, Status: domain.StatusPending, IdempotencyKey: &key,
			CreatedAt: time.Now(), UpdatedAt: time.Now()}
	}
	if err := repo.Create(ctx, mk(newUUID())); err != nil { t.Fatal(err) }
	if err := repo.Create(ctx, mk(newUUID())); err == nil { t.Fatal("expected unique violation") }
}
```

Provide `newTestPool` in `internal/adapter/repository/postgres/main_test.go` using `testcontainers-go` (postgres module) + `db.Migrate`, and a `newUUID()` helper.

- [ ] **Step 2: Run to verify fail**

Run: `go test -tags=integration ./internal/adapter/repository/postgres/...`
Expected: FAIL (NewNotificationRepo undefined).

- [ ] **Step 3: Implement `notification_repo.go`**

```go
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type NotificationRepo struct{ pool *pgxpool.Pool }

func NewNotificationRepo(p *pgxpool.Pool) *NotificationRepo { return &NotificationRepo{pool: p} }

func (r *NotificationRepo) Create(ctx context.Context, n domain.Notification) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO notifications
		(id, batch_id, channel, recipient, content, template_id, variables, priority, status,
		 idempotency_key, scheduled_at, attempts, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		n.ID, n.BatchID, n.Channel, n.Recipient, n.Content, n.TemplateID, n.Variables, n.Priority,
		n.Status, n.IdempotencyKey, n.ScheduledAt, n.Attempts, n.CreatedAt, n.UpdatedAt)
	if isUniqueViolation(err) {
		return domain.ErrDuplicate
	}
	return err
}

func (r *NotificationRepo) CreateBatch(ctx context.Context, b domain.Batch, ns []domain.Notification) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO batches (id,total,created_at) VALUES ($1,$2,$3)`, b.ID, b.Total, b.CreatedAt); err != nil {
		return err
	}
	for _, n := range ns {
		if _, err := tx.Exec(ctx, `
			INSERT INTO notifications
			(id, batch_id, channel, recipient, content, template_id, variables, priority, status,
			 idempotency_key, scheduled_at, attempts, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			n.ID, n.BatchID, n.Channel, n.Recipient, n.Content, n.TemplateID, n.Variables, n.Priority,
			n.Status, n.IdempotencyKey, n.ScheduledAt, n.Attempts, n.CreatedAt, n.UpdatedAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *NotificationRepo) Get(ctx context.Context, id string) (domain.Notification, error) {
	return r.scanOne(ctx, `SELECT `+notifCols+` FROM notifications WHERE id=$1`, id)
}

func (r *NotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (domain.Notification, error) {
	return r.scanOne(ctx, `SELECT `+notifCols+` FROM notifications WHERE idempotency_key=$1`, key)
}

func (r *NotificationRepo) UpdateStatus(ctx context.Context, id string, s domain.Status, le *string, pmid *string, attempts int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE notifications SET status=$2, last_error=$3, provider_message_id=COALESCE($4, provider_message_id),
		attempts=$5, updated_at=now() WHERE id=$1`, id, s, le, pmid, attempts)
	return err
}

func (r *NotificationRepo) List(ctx context.Context, f usecase.NotificationFilter) ([]domain.Notification, int, error) {
	// build dynamic WHERE
	where := "WHERE 1=1"
	args := []any{}
	add := func(cond string, v any) { args = append(args, v); where += cond }
	if f.Status != nil { add(" AND status=$"+itoa(len(args)+1), *f.Status) }
	if f.Channel != nil { add(" AND channel=$"+itoa(len(args)+1), *f.Channel) }
	if f.From != nil { add(" AND created_at>=$"+itoa(len(args)+1), *f.From) }
	if f.To != nil { add(" AND created_at<=$"+itoa(len(args)+1), *f.To) }

	var total int
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM notifications `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	q := `SELECT ` + notifCols + ` FROM notifications ` + where +
		` ORDER BY created_at DESC LIMIT $` + itoa(len(args)+1) + ` OFFSET $` + itoa(len(args)+2)
	args = append(args, f.Limit, f.Offset)
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := []domain.Notification{}
	for rows.Next() {
		n, err := scanNotif(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, n)
	}
	return out, total, rows.Err()
}

func (r *NotificationRepo) scanOne(ctx context.Context, q string, args ...any) (domain.Notification, error) {
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return domain.Notification{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return domain.Notification{}, domain.ErrNotFound
	}
	return scanNotif(rows)
}

const notifCols = `id, batch_id, channel, recipient, content, template_id, variables, priority, status,
	idempotency_key, scheduled_at, attempts, last_error, provider_message_id, created_at, updated_at`

func scanNotif(rows pgx.Rows) (domain.Notification, error) {
	var n domain.Notification
	err := rows.Scan(&n.ID, &n.BatchID, &n.Channel, &n.Recipient, &n.Content, &n.TemplateID,
		&n.Variables, &n.Priority, &n.Status, &n.IdempotencyKey, &n.ScheduledAt, &n.Attempts,
		&n.LastError, &n.ProviderMessageID, &n.CreatedAt, &n.UpdatedAt)
	return n, err
}

func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	return err != nil && errors.As(err, &pgErr) && pgErr.SQLState() == "23505"
}
```

Add tiny helpers in `internal/adapter/repository/postgres/util.go`:
```go
package postgres

import "strconv"

func itoa(i int) string { return strconv.Itoa(i) }
```

- [ ] **Step 4: Implement `template_repo.go`, `batch_repo.go`, `attempt_repo.go`**

Follow the same pattern. `BatchRepo.StatusCounts` runs:
```sql
SELECT status, count(*) FROM notifications WHERE batch_id=$1 GROUP BY status
```
`AttemptRepo.Add` inserts into `delivery_attempts`. `TemplateRepo` implements Create/Get/List.

- [ ] **Step 5: Run to verify pass**

Run: `go test -tags=integration ./internal/adapter/repository/postgres/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/repository/postgres internal/infrastructure/db && git commit -m "feat: postgres repositories"
```

---

## Phase 4 — Redis adapters

### Task 4.1: Redis client + priority queue

**Files:**
- Create: `internal/infrastructure/redisclient/redisclient.go`, `internal/adapter/queue/redis/queue.go`
- Test: `internal/adapter/queue/redis/queue_integration_test.go` (tag `integration`)

- [ ] **Step 1: Write failing integration test**

```go
//go:build integration

package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func TestQueue_PriorityOrder(t *testing.T) {
	rdb := newTestRedis(t) // testcontainers redis helper
	q := New(rdb)
	ctx := context.Background()
	_ = q.Enqueue(ctx, usecase.QueueItem{NotificationID: "low", Priority: domain.PriorityLow})
	_ = q.Enqueue(ctx, usecase.QueueItem{NotificationID: "high", Priority: domain.PriorityHigh})
	item, ok, err := q.Dequeue(ctx, time.Second)
	if err != nil || !ok { t.Fatalf("dequeue: %v ok=%v", err, ok) }
	if item.NotificationID != "high" { t.Fatalf("want high first, got %s", item.NotificationID) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test -tags=integration ./internal/adapter/queue/redis/...`
Expected: FAIL.

- [ ] **Step 3: Implement queue**

```go
package redisqueue

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type Queue struct{ rdb *redis.Client }

func New(rdb *redis.Client) *Queue { return &Queue{rdb: rdb} }

func key(p domain.Priority) string { return "queue:" + string(p) }

// priority order for draining
var order = []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}

func (q *Queue) Enqueue(ctx context.Context, item usecase.QueueItem) error {
	return q.rdb.LPush(ctx, key(item.Priority), item.NotificationID).Err()
}

func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (usecase.QueueItem, bool, error) {
	keys := []string{key(domain.PriorityHigh), key(domain.PriorityNormal), key(domain.PriorityLow)}
	res, err := q.rdb.BRPop(ctx, timeout, keys...).Result()
	if err == redis.Nil {
		return usecase.QueueItem{}, false, nil
	}
	if err != nil {
		return usecase.QueueItem{}, false, err
	}
	// res[0]=key, res[1]=value
	p := domain.Priority(strings.TrimPrefix(res[0], "queue:"))
	return usecase.QueueItem{NotificationID: res[1], Priority: p}, true, nil
}

func (q *Queue) Remove(ctx context.Context, id string) error {
	for _, p := range order {
		if err := q.rdb.LRem(ctx, key(p), 0, id).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Depth(ctx context.Context, p domain.Priority) (int64, error) {
	return q.rdb.LLen(ctx, key(p)).Result()
}
```

> Anti-starvation note: `BRPop` with keys ordered high→normal→low always prefers higher priority but, because it blocks across all keys, lower priorities are still served whenever higher queues are empty. This satisfies "drain high first" without hard starvation in practice. Document this in the README.

`redisclient.go`:
```go
package redisclient

import "github.com/redis/go-redis/v9"

func New(addr string) *redis.Client { return redis.NewClient(&redis.Options{Addr: addr}) }
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags=integration ./internal/adapter/queue/redis/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/queue/redis internal/infrastructure/redisclient && git commit -m "feat: redis priority queue"
```

### Task 4.2: Retry queue, DLQ, scheduled store (Redis ZSET)

**Files:**
- Create: `internal/adapter/queue/redis/retry.go`, `dlq.go`, `scheduled.go`
- Test: `internal/adapter/queue/redis/retry_integration_test.go`

- [ ] **Step 1: Write failing integration test**

```go
//go:build integration

package redisqueue

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func TestRetry_DuePromote(t *testing.T) {
	rdb := newTestRedis(t)
	q := New(rdb)
	rq := NewRetry(rdb)
	ctx := context.Background()
	past := time.Now().Add(-time.Second)
	_ = rq.Schedule(ctx, usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}, past)
	n, err := rq.DuePromote(ctx, time.Now(), q)
	if err != nil { t.Fatal(err) }
	if n != 1 { t.Fatalf("want 1 promoted, got %d", n) }
	item, ok, _ := q.Dequeue(ctx, time.Second)
	if !ok || item.NotificationID != "n1" { t.Fatal("item not promoted to queue") }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test -tags=integration ./internal/adapter/queue/redis/... -run TestRetry`
Expected: FAIL.

- [ ] **Step 3: Implement `retry.go`** (scheduled.go mirrors it with a different key)

```go
package redisqueue

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

const retryKey = "retry:zset"

type Retry struct{ rdb *redis.Client }

func NewRetry(rdb *redis.Client) *Retry { return &Retry{rdb: rdb} }

// member encodes "priority|id"
func encode(item usecase.QueueItem) string { return string(item.Priority) + "|" + item.NotificationID }
func decode(m string) usecase.QueueItem {
	parts := strings.SplitN(m, "|", 2)
	return usecase.QueueItem{Priority: domain.Priority(parts[0]), NotificationID: parts[1]}
}

func (r *Retry) Schedule(ctx context.Context, item usecase.QueueItem, at time.Time) error {
	return r.rdb.ZAdd(ctx, retryKey, redis.Z{Score: float64(at.Unix()), Member: encode(item)}).Err()
}

func (r *Retry) DuePromote(ctx context.Context, now time.Time, dst usecase.Queue) (int, error) {
	members, err := r.rdb.ZRangeByScore(ctx, retryKey, &redis.ZRangeBy{
		Min: "-inf", Max: strconvI(now.Unix()),
	}).Result()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, m := range members {
		// remove first to avoid double-promote across schedulers/workers
		removed, err := r.rdb.ZRem(ctx, retryKey, m).Result()
		if err != nil || removed == 0 {
			continue
		}
		if err := dst.Enqueue(ctx, decode(m)); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (r *Retry) Size(ctx context.Context) (int64, error) {
	return r.rdb.ZCard(ctx, retryKey).Result()
}
```

Add `strconvI`:
```go
func strconvI(i int64) string { return strconv.FormatInt(i, 10) }
```

`dlq.go`:
```go
package redisqueue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const dlqKey = "dlq:list"

type DLQ struct{ rdb *redis.Client }
func NewDLQ(rdb *redis.Client) *DLQ { return &DLQ{rdb: rdb} }
func (d *DLQ) Push(ctx context.Context, id, reason string) error {
	return d.rdb.LPush(ctx, dlqKey, id+"|"+reason).Err()
}
func (d *DLQ) Size(ctx context.Context) (int64, error) { return d.rdb.LLen(ctx, dlqKey).Result() }
```

`scheduled.go`: identical structure to `retry.go` but `key = "scheduled:zset"`, plus a `Remove(ctx, id)` that `ZRem`s any member ending in `|id` (scan members, match suffix).

- [ ] **Step 4: Run to verify pass**

Run: `go test -tags=integration ./internal/adapter/queue/redis/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/queue/redis && git commit -m "feat: redis retry queue, dlq, scheduled store"
```

### Task 4.3: Rate limiter (token bucket) + idempotency store

**Files:**
- Create: `internal/adapter/ratelimit/redis/limiter.go`, `internal/adapter/idempotency/redis/store.go`
- Test: integration tests for each

- [ ] **Step 1: Write failing test (limiter)**

```go
//go:build integration

package redisratelimit

import (
	"context"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestLimiter_BlocksOverCapacity(t *testing.T) {
	rdb := newTestRedis(t)
	l := New(rdb, 2) // 2 per second
	ctx := context.Background()
	a1, _ := l.Allow(ctx, domain.ChannelSMS)
	a2, _ := l.Allow(ctx, domain.ChannelSMS)
	a3, _ := l.Allow(ctx, domain.ChannelSMS)
	if !a1 || !a2 { t.Fatal("first two should pass") }
	if a3 { t.Fatal("third should be blocked") }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test -tags=integration ./internal/adapter/ratelimit/redis/...`
Expected: FAIL.

- [ ] **Step 3: Implement limiter (sliding/fixed-window via INCR+EXPIRE)**

```go
package redisratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

type Limiter struct {
	rdb     *redis.Client
	perSec  int
}

func New(rdb *redis.Client, perSec int) *Limiter { return &Limiter{rdb: rdb, perSec: perSec} }

// Allow uses a 1-second fixed window counter per channel.
func (l *Limiter) Allow(ctx context.Context, ch domain.Channel) (bool, error) {
	window := time.Now().Unix()
	k := fmt.Sprintf("rate:%s:%d", ch, window)
	n, err := l.rdb.Incr(ctx, k).Result()
	if err != nil {
		return false, err
	}
	if n == 1 {
		_ = l.rdb.Expire(ctx, k, 2*time.Second).Err()
	}
	return n <= int64(l.perSec), nil
}
```

- [ ] **Step 4: Implement idempotency store**

```go
package redisidem

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client, ttl time.Duration) *Store { return &Store{rdb: rdb, ttl: ttl} }

// Remember sets key→id only if absent. Returns existing id if already present.
func (s *Store) Remember(ctx context.Context, key, id string) (string, bool, error) {
	ok, err := s.rdb.SetNX(ctx, "idem:"+key, id, s.ttl).Result()
	if err != nil {
		return "", false, err
	}
	if ok {
		return "", false, nil
	}
	existing, err := s.rdb.Get(ctx, "idem:"+key).Result()
	if err != nil {
		return "", false, err
	}
	return existing, true, nil
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test -tags=integration ./internal/adapter/ratelimit/redis/... ./internal/adapter/idempotency/redis/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/ratelimit internal/adapter/idempotency && git commit -m "feat: redis rate limiter and idempotency store"
```

---

## Phase 5 — Provider adapter

### Task 5.1: webhook.site provider client

**Files:**
- Create: `internal/adapter/provider/webhook/client.go`
- Test: `internal/adapter/provider/webhook/client_test.go` (uses `httptest`, no tag)

- [ ] **Step 1: Write failing test**

```go
package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

func TestSend_Accepts202(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m1","status":"accepted","timestamp":"2026-06-09T00:00:00Z"}`))
	}))
	defer srv.Close()
	c := New(srv.URL, srv.Client())
	resp, err := c.Send(context.Background(), domain.Notification{Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi"})
	if err != nil { t.Fatalf("err: %v", err) }
	if resp.MessageID != "m1" { t.Fatalf("got %q", resp.MessageID) }
}

func TestSend_Non202IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := New(srv.URL, srv.Client())
	if _, err := c.Send(context.Background(), domain.Notification{Channel: domain.ChannelSMS, Recipient: "+1", Content: "hi"}); err == nil {
		t.Fatal("expected error on 500")
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/adapter/provider/webhook/...`
Expected: FAIL.

- [ ] **Step 3: Implement**

```go
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type Client struct {
	url  string
	http *http.Client
}

func New(url string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{url: url, http: hc}
}

type reqBody struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

func (c *Client) Send(ctx context.Context, n domain.Notification) (usecase.ProviderResponse, error) {
	body, _ := json.Marshal(reqBody{To: n.Recipient, Channel: string(n.Channel), Content: n.Content})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return usecase.ProviderResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return usecase.ProviderResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return usecase.ProviderResponse{}, fmt.Errorf("provider returned status %d", resp.StatusCode)
	}
	var out usecase.ProviderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return usecase.ProviderResponse{}, fmt.Errorf("decode provider response: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/adapter/provider/webhook/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/provider && git commit -m "feat: webhook.site provider client"
```

---

## Phase 6 — Observability

### Task 6.1: Logging, metrics, tracing setup

**Files:**
- Create: `internal/infrastructure/observability/logging.go`, `metrics.go`, `tracing.go`
- Test: `internal/infrastructure/observability/metrics_test.go`

- [ ] **Step 1: Write failing test (metrics registry)**

```go
package observability

import "testing"

func TestMetrics_Register(t *testing.T) {
	m := NewMetrics()
	m.Enqueued.WithLabelValues("sms", "high").Inc()
	m.Delivered.WithLabelValues("sms").Inc()
	// no panic / duplicate registration == pass
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/infrastructure/observability/...`
Expected: FAIL.

- [ ] **Step 3: Implement `metrics.go`**

```go
package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Enqueued    *prometheus.CounterVec
	Delivered   *prometheus.CounterVec
	Failed      *prometheus.CounterVec
	Retried     *prometheus.CounterVec
	RateLimited *prometheus.CounterVec
	Latency     *prometheus.HistogramVec
	QueueDepth  *prometheus.GaugeVec
	DLQSize     prometheus.Gauge
	InFlight    prometheus.Gauge
	reg         *prometheus.Registry
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg:         reg,
		Enqueued:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_enqueued_total"}, []string{"channel", "priority"}),
		Delivered:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_delivered_total"}, []string{"channel"}),
		Failed:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_failed_total"}, []string{"channel"}),
		Retried:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_retried_total"}, []string{"channel"}),
		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notif_rate_limited_total"}, []string{"channel"}),
		Latency:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notif_delivery_latency_seconds", Buckets: prometheus.DefBuckets}, []string{"channel"}),
		QueueDepth:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "notif_queue_depth"}, []string{"priority"}),
		DLQSize:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "notif_dlq_size"}),
		InFlight:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "notif_in_flight"}),
	}
	reg.MustRegister(m.Enqueued, m.Delivered, m.Failed, m.Retried, m.RateLimited, m.Latency, m.QueueDepth, m.DLQSize, m.InFlight)
	return m
}

func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
```

- [ ] **Step 4: Implement `logging.go` and `tracing.go`**

`logging.go`:
```go
package observability

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey string

const correlationKey ctxKey = "correlation_id"

func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(level))
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey, id)
}

func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}
	return ""
}
```

`tracing.go`: standard OTel OTLP/HTTP tracer provider init returning a `func(context.Context) error` shutdown. Use `go.opentelemetry.io/otel`, `otlptracehttp`, `sdk/trace`, `sdk/resource`. If `OTELEndpoint` is empty, return a no-op shutdown.

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/infrastructure/observability/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/infrastructure/observability && git commit -m "feat: logging, metrics, tracing setup"
```

---

## Phase 7 — HTTP adapter (Gin) + WebSocket

### Task 7.1: DTOs and middleware

**Files:**
- Create: `internal/adapter/http/dto.go`, `internal/adapter/http/middleware.go`
- Test: `internal/adapter/http/middleware_test.go`

- [ ] **Step 1: Write failing test (correlation middleware sets header)**

```go
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
	r.GET("/x", func(c *gin.Context) { c.Status(200) })
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/x", nil)
	r.ServeHTTP(w, req)
	if w.Header().Get("X-Correlation-ID") == "" {
		t.Fatal("expected correlation id header")
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/adapter/http/...`
Expected: FAIL.

- [ ] **Step 3: Implement `middleware.go`**

```go
package httpapi

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/ibrahim-bg/notifier/internal/infrastructure/observability"
)

func CorrelationID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Correlation-ID")
		if id == "" {
			id = uuid.NewString()
		}
		c.Header("X-Correlation-ID", id)
		c.Request = c.Request.WithContext(observability.WithCorrelationID(c.Request.Context(), id))
		c.Next()
	}
}
```

- [ ] **Step 4: Implement `dto.go`**

Request/response structs with Gin binding tags: `CreateNotificationRequest`, `BatchRequest`, `CreateTemplateRequest`, `NotificationResponse`, `BatchStatusResponse`, plus a `toCreateInput()` mapper and `fromDomain()` mapper. Include validation binding tags (`binding:"required"`).

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/adapter/http/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/http && git commit -m "feat: http dto + correlation middleware"
```

### Task 7.2: Handlers + router

**Files:**
- Create: `internal/adapter/http/handlers.go`, `internal/adapter/http/router.go`
- Test: `internal/adapter/http/handlers_test.go`

- [ ] **Step 1: Write failing test (create returns 201 with id)**

```go
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

// stub create service exposing the same Execute signature
type stubCreate struct{}
func (stubCreate) Execute(_ context.Context, in usecase.CreateInput) (domain.Notification, error) {
	return domain.Notification{ID: "n1", Channel: in.Channel, Status: domain.StatusQueued}, nil
}

func TestCreateHandler_201(t *testing.T) {
	h := &Handlers{Create: stubCreate{}}
	r := newTestRouter(h)
	body := `{"channel":"sms","recipient":"+1","content":"hi","priority":"high"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/notifications", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated { t.Fatalf("want 201, got %d: %s", w.Code, w.Body.String()) }
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["id"] != "n1" { t.Fatalf("want id n1, got %v", resp["id"]) }
}
```

Define handler-facing interfaces (so handlers depend on small interfaces, not concrete interactors):
```go
type createService interface {
	Execute(ctx context.Context, in usecase.CreateInput) (domain.Notification, error)
}
```
Mirror for each interactor used by handlers. `newTestRouter(h)` builds the Gin engine with routes.

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/adapter/http/... -run TestCreateHandler`
Expected: FAIL.

- [ ] **Step 3: Implement handlers + router**

`handlers.go` holds a `Handlers` struct with fields for each service interface and methods: `CreateNotification`, `CreateBatch`, `GetNotification`, `ListNotifications`, `CancelNotification`, `GetBatch`, `CreateTemplate`, `ListTemplates`. Each binds the request, calls the interactor, maps domain errors to HTTP codes (`ErrValidation`→400, `ErrNotFound`→404, `ErrNotCancellable`→409, `ErrDuplicate`→409, else 500), and writes the response.

`router.go`:
```go
package httpapi

import (
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func NewRouter(h *Handlers, promHandler gin.HandlerFunc, health *Health) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), CorrelationID())
	v1 := r.Group("/api/v1")
	v1.POST("/notifications", h.CreateNotification)
	v1.POST("/notifications/batch", h.CreateBatch)
	v1.GET("/notifications", h.ListNotifications)
	v1.GET("/notifications/:id", h.GetNotification)
	v1.DELETE("/notifications/:id", h.CancelNotification)
	v1.GET("/batches/:id", h.GetBatch)
	v1.POST("/templates", h.CreateTemplate)
	v1.GET("/templates", h.ListTemplates)
	r.GET("/healthz", health.Live)
	r.GET("/readyz", health.Ready)
	r.GET("/metrics", promHandler)
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return r
}
```

Add `health.go` with `Health{DB pinger, Redis pinger}` and `Live`/`Ready` handlers.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/adapter/http/...`
Expected: PASS.

- [ ] **Step 5: Add Swagger annotations + generate**

Annotate handlers with swaggo comments. Run `make swagger`. Commit generated `api/openapi`.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/http api/openapi && git commit -m "feat: http handlers, router, swagger"
```

### Task 7.3: WebSocket hub (EventPublisher)

**Files:**
- Create: `internal/adapter/ws/hub.go`, `internal/adapter/ws/handler.go`
- Test: `internal/adapter/ws/hub_test.go`

- [ ] **Step 1: Write failing test (publish reaches subscriber)**

```go
package ws

import (
	"context"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

func TestHub_PublishToSubscriber(t *testing.T) {
	h := NewHub()
	ch := h.Subscribe("n1")
	defer h.Unsubscribe("n1", ch)
	h.Publish(context.Background(), usecase.StatusEvent{NotificationID: "n1", Status: domain.StatusDelivered, At: time.Now()})
	select {
	case e := <-ch:
		if e.Status != domain.StatusDelivered { t.Fatalf("got %s", e.Status) }
	case <-time.After(time.Second):
		t.Fatal("no event received")
	}
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/adapter/ws/...`
Expected: FAIL.

- [ ] **Step 3: Implement `hub.go`**

```go
package ws

import (
	"context"
	"sync"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type Hub struct {
	mu   sync.RWMutex
	subs map[string]map[chan usecase.StatusEvent]struct{}
}

func NewHub() *Hub { return &Hub{subs: map[string]map[chan usecase.StatusEvent]struct{}{}} }

func (h *Hub) Subscribe(id string) chan usecase.StatusEvent {
	ch := make(chan usecase.StatusEvent, 8)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[id] == nil {
		h.subs[id] = map[chan usecase.StatusEvent]struct{}{}
	}
	h.subs[id][ch] = struct{}{}
	return ch
}

func (h *Hub) Unsubscribe(id string, ch chan usecase.StatusEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if m := h.subs[id]; m != nil {
		delete(m, ch)
		close(ch)
		if len(m) == 0 {
			delete(h.subs, id)
		}
	}
}

func (h *Hub) Publish(_ context.Context, e usecase.StatusEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	deliver := func(id string) {
		for ch := range h.subs[id] {
			select {
			case ch <- e:
			default: // drop if slow consumer
			}
		}
	}
	deliver(e.NotificationID)
	if e.BatchID != nil {
		deliver(*e.BatchID)
	}
}
```

> Cross-process note: workers and the API are separate processes, so an in-memory hub only sees events from its own process. To deliver worker status changes to API WebSocket clients, the worker publishes events to a Redis Pub/Sub channel and the API subscribes and fans them into its local Hub. Implement `internal/adapter/ws/redis_bridge.go`: worker-side `RedisPublisher` implements `usecase.EventPublisher` by `PUBLISH`ing JSON; API-side bridge `SUBSCRIBE`s and calls `Hub.Publish`. The worker is wired with `RedisPublisher`; the API with `Hub` + bridge.

- [ ] **Step 4: Implement `handler.go`** (gorilla/websocket upgrade, read `?id=` query, subscribe, stream events as JSON until client disconnects). Implement `redis_bridge.go` per the note.

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/adapter/ws/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/ws && git commit -m "feat: websocket hub + redis pub/sub bridge"
```

---

## Phase 8 — Worker binary

### Task 8.1: Worker loop + cmd/worker

**Files:**
- Create: `internal/worker/loop.go`, `cmd/worker/main.go`
- Test: `internal/worker/loop_test.go`

- [ ] **Step 1: Write failing test (loop processes one item then stops on ctx cancel)**

```go
package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type stubQueue struct{ items chan usecase.QueueItem }
func (s *stubQueue) Enqueue(context.Context, usecase.QueueItem) error { return nil }
func (s *stubQueue) Dequeue(ctx context.Context, _ time.Duration) (usecase.QueueItem, bool, error) {
	select {
	case it := <-s.items:
		return it, true, nil
	case <-ctx.Done():
		return usecase.QueueItem{}, false, ctx.Err()
	}
}
func (s *stubQueue) Remove(context.Context, string) error { return nil }
func (s *stubQueue) Depth(context.Context, domain.Priority) (int64, error) { return 0, nil }

func TestLoop_ProcessesItem(t *testing.T) {
	q := &stubQueue{items: make(chan usecase.QueueItem, 1)}
	q.items <- usecase.QueueItem{NotificationID: "n1", Priority: domain.PriorityNormal}
	var processed int32
	proc := func(_ context.Context, _ usecase.QueueItem) error { atomic.AddInt32(&processed, 1); return nil }
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	Run(ctx, q, proc, 1)
	if atomic.LoadInt32(&processed) != 1 { t.Fatalf("want 1 processed, got %d", processed) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/worker/...`
Expected: FAIL.

- [ ] **Step 3: Implement `loop.go`**

```go
package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ibrahim-bg/notifier/internal/usecase"
)

type ProcessFunc func(ctx context.Context, item usecase.QueueItem) error

func Run(ctx context.Context, q usecase.Queue, process ProcessFunc, concurrency int) {
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				item, ok, err := q.Dequeue(ctx, time.Second)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					continue
				}
				if !ok {
					continue
				}
				_ = process(ctx, item) // ProcessNotification handles retry/DLQ internally
			}
		}()
	}
	wg.Wait()
}
```

- [ ] **Step 4: Implement `cmd/worker/main.go`**

Composition root: load config, connect Postgres + Redis, build repos/adapters, construct `ProcessNotification` (with `RedisPublisher`), start a retry-promotion ticker (`Retry.DuePromote` every second into the queue), and call `worker.Run`. Handle SIGINT/SIGTERM for graceful shutdown via context cancel.

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/worker/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/worker cmd/worker && git commit -m "feat: worker loop and cmd/worker"
```

---

## Phase 9 — Scheduler binary

### Task 9.1: Scheduler loop + cmd/scheduler

**Files:**
- Create: `internal/scheduler/scheduler.go`, `cmd/scheduler/main.go`
- Test: `internal/scheduler/scheduler_test.go`

- [ ] **Step 1: Write failing test**

```go
package scheduler

import (
	"context"
	"testing"
	"time"
)

type stubPromoter struct{ calls int }
func (s *stubPromoter) Tick(context.Context, time.Time) (int, error) { s.calls++; return 0, nil }

func TestScheduler_TicksUntilCancel(t *testing.T) {
	p := &stubPromoter{}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(120 * time.Millisecond); cancel() }()
	Run(ctx, p, 50*time.Millisecond)
	if p.calls < 1 { t.Fatalf("expected at least 1 tick, got %d", p.calls) }
}
```

- [ ] **Step 2: Run to verify fail**

Run: `go test ./internal/scheduler/...`
Expected: FAIL.

- [ ] **Step 3: Implement**

```go
package scheduler

import (
	"context"
	"time"
)

type Promoter interface {
	Tick(ctx context.Context, now time.Time) (int, error)
}

func Run(ctx context.Context, p Promoter, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			_, _ = p.Tick(ctx, now)
		}
	}
}
```

The concrete promoter (in `cmd/scheduler/main.go` or a small adapter) calls `ScheduledStore.DuePromote(ctx, now, queue)` and also `RetryQueue.DuePromote` (so retries flow even if no worker runs a ticker — choose one owner; here the scheduler owns scheduled promotion, the worker owns retry promotion, as wired in Task 8.4).

- [ ] **Step 4: Implement `cmd/scheduler/main.go`** — composition root: config, Redis, build `ScheduledStore` + `Queue`, wrap in a promoter, run.

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/scheduler/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/scheduler cmd/scheduler && git commit -m "feat: scheduler loop and cmd/scheduler"
```

---

## Phase 10 — API binary composition root

### Task 10.1: cmd/api/main.go

**Files:**
- Create: `cmd/api/main.go`, `internal/infrastructure/db/migrations_embed.go`

- [ ] **Step 1: Implement migrations embed**

```go
package db

import "embed"

//go:embed all:../../../migrations
var MigrationsFS embed.FS
```

> Adjust the embed path so it resolves from the package directory to `migrations/`. If the relative path is awkward, instead place the embed in a top-level `internal/migrations` package that re-exports `embed.FS`.

- [ ] **Step 2: Implement `cmd/api/main.go`**

Composition root:
1. Load config; build logger; init tracing (shutdown deferred).
2. Connect Postgres; run `db.Migrate`.
3. Connect Redis.
4. Build repos (postgres) + adapters (redis queue/retry/dlq/scheduled, ratelimit, idempotency).
5. Build metrics; build WS hub + start Redis→Hub bridge.
6. Construct interactors (`CreateNotification`, `CreateBatch`, `CancelNotification`, `ListNotifications`, `GetNotification`, `GetBatchStatus`, `CreateTemplate`, `ListTemplates`) injecting concrete adapters; `IDGen = uuid.NewString`; `Clock` = real clock.
7. Build `Handlers`, `Health`, prom handler (`promhttp.HandlerFor(metrics.Registry(), ...)` wrapped for gin), router.
8. Start HTTP server; graceful shutdown on signal.

- [ ] **Step 3: Verify build**

Run: `make build`
Expected: all three binaries build.

- [ ] **Step 4: Commit**

```bash
git add cmd/api internal/infrastructure/db && git commit -m "feat: cmd/api composition root"
```

---

## Phase 11 — Docker Compose & dashboards

### Task 11.1: Dockerfiles + compose stack

**Files:**
- Create: `Dockerfile`, `deploy/docker-compose.yml`, `deploy/prometheus.yml`, `deploy/grafana/` (provisioning + dashboard JSON)

- [ ] **Step 1: Multi-stage `Dockerfile`** (builds all three binaries; `CMD` overridden per service in compose)

```dockerfile
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -o /out/worker ./cmd/worker \
 && CGO_ENABLED=0 go build -o /out/scheduler ./cmd/scheduler

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /app/
ENTRYPOINT ["/app/api"]
```

- [ ] **Step 2: `deploy/docker-compose.yml`** with services: `postgres`, `redis`, `api` (build, depends_on healthy pg+redis, ports 8080), `worker` (entrypoint `/app/worker`, `deploy.replicas: 2`), `scheduler` (entrypoint `/app/scheduler`), `prometheus`, `grafana`, `jaeger`. Wire env from `.env`. Add healthchecks for pg/redis.

- [ ] **Step 3: `deploy/prometheus.yml`** scraping `api:8080/metrics`.

- [ ] **Step 4: Grafana provisioning** — datasource (Prometheus) + a dashboard JSON with panels for queue depth, delivered/failed rate, latency, DLQ size.

- [ ] **Step 5: Verify stack boots**

Run: `make up` then in another shell:
```bash
curl -s localhost:8080/healthz
curl -s -X POST localhost:8080/api/v1/notifications -H 'content-type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hi","priority":"high"}'
```
Expected: `200` health; `201` create with an id; notification reaches `delivered` (check `GET /api/v1/notifications/{id}`) once webhook.site URL is configured via env `PROVIDER_URL`.

> Add `PROVIDER_URL` to config (Task 0.2 follow-up) and `.env.example`; the webhook provider client reads it. Default to a placeholder; the README explains creating a webhook.site UUID.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile deploy && git commit -m "feat: docker compose stack with observability"
```

---

## Phase 12 — Docs & final verification

### Task 12.1: README + final pass

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README** — overview, architecture diagram (from spec), prerequisites, `make up` quickstart, configuring webhook.site `PROVIDER_URL`, API examples (curl for create/batch/get/list/cancel/template/ws), how to run tests (`make test`, `make test-integration`), and the design rationale (priority queue, rate limiting, retry/DLQ, idempotency).

- [ ] **Step 2: Run full checks**

Run:
```bash
make fmt
make lint
make test
make test-integration
```
Expected: all pass clean.

- [ ] **Step 3: Commit**

```bash
git add README.md && git commit -m "docs: README with setup, architecture, API examples"
```

---

## Self-Review (completed during planning)

**Spec coverage check:**
- Notification API (create/batch/query/cancel/list+filter+pagination) → Tasks 2.3–2.7, 7.2. ✓
- Async queue workers → Phase 8. ✓
- Rate limiting 100/s/channel → Task 4.3. ✓
- Priority queues → Task 4.1. ✓
- Content validation → Task 1.2. ✓
- Idempotency → Task 4.3 + 2.3 (DB unique in 3.1). ✓
- Delivery + fixed-interval retry + DLQ → Task 2.6, 4.2. ✓
- Observability (metrics/logging/correlation/health/tracing) → Phase 6, Task 7.2. ✓
- webhook.site provider → Phase 5. ✓
- Scheduled notifications → Task 4.2 (scheduled.go), Phase 9. ✓
- Template system → Task 1.3, 2.7. ✓
- WebSocket → Task 7.3. ✓
- Deliverables (compose, swagger, migrations, tests, README) → Phases 3, 7, 11, 12. ✓

**Open notes for the implementer (resolve while building, not blockers):**
1. `PROVIDER_URL` config field is introduced in Phase 11 — add it to `Config` (Task 0.2) and `webhook.New` wiring when you reach the composition roots.
2. Choose a single owner per promotion loop: scheduler owns scheduled→queue; worker process owns retry→queue (or run a dedicated promotion goroutine in the worker). Keep it consistent so items aren't double-promoted (the `ZRem`-before-enqueue pattern already guards correctness).
3. The fake test typo placeholder in Task 1.2 (`ChannelالسMS`) must be written as `ChannelSMS`.
