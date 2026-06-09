# Event-Driven Notification System — Design Spec

**Project:** Lead Software Engineer Assessment
**Date:** 2026-06-09
**Language:** Go (1.2x)

## 1. Overview

A scalable, event-driven notification system that accepts notification requests over a
REST API, processes them asynchronously through priority queue workers, delivers them to
an external provider (simulated via webhook.site) across SMS / Email / Push channels, and
provides real-time status tracking and observability.

The system is designed for high throughput and burst traffic, with per-channel rate
limiting, idempotent creation, fixed-interval retries with a dead-letter queue, scheduled
delivery, message templates, and live status updates over WebSocket.

## 2. Goals & Scope

### In scope (core requirements)
- Notification Management API: create (single + batch up to 1000), query by id / batch id,
  cancel pending, list with filtering (status, channel, date range) + pagination.
- Processing engine: async queue workers, per-channel rate limiting (max 100 msg/s/channel),
  priority queues (high/normal/low), content validation, idempotency.
- Delivery & retry: fixed-interval retry with max attempts, then dead-letter queue.
- Observability: Prometheus metrics, structured JSON logging with correlation IDs,
  health checks, OpenTelemetry distributed tracing.

### In scope (selected bonuses)
- Scheduled notifications (future delivery).
- Template system (variable substitution).
- WebSocket real-time status updates.
- Failure handling / DLQ (covered by retry design).
- Distributed tracing (covered by observability).

### Out of scope (explicit decisions)
- GitHub Actions CI/CD — deferred. Linting runs locally via `make lint`; a workflow calling
  `make lint test` can be added later in ~15 lines.
- API authentication and WebSocket authentication — not required by the brief; endpoints are
  open. (An optional API-key middleware stub may be added if desired, but is not planned.)

## 3. Technology Choices

| Concern        | Choice |
|----------------|--------|
| Language       | Go 1.2x |
| HTTP framework | Gin |
| System of record | PostgreSQL |
| Queue / coordination | Redis (priority queues, rate limiter, idempotency cache, scheduled set, retry set, DLQ) |
| Metrics        | Prometheus + Grafana (provisioned dashboard) |
| Tracing        | OpenTelemetry → Jaeger |
| Logging        | `slog` (structured JSON) with correlation IDs |
| Migrations     | golang-migrate (versioned SQL) |
| API docs       | Swagger / OpenAPI (swaggo) |
| Lint           | golangci-lint |
| Tests          | Go stdlib testing + testcontainers-go + httptest |

## 4. Architecture

### 4.1 Process topology
Shared `internal/` libraries with three runnable binaries, each its own composition root:

- `cmd/api` — Gin REST API + WebSocket + Swagger. Validates, persists, enqueues. **N replicas.**
- `cmd/worker` — dequeues, rate-limits, delivers to provider, updates state, emits events. **N replicas.**
- `cmd/scheduler` — promotes due scheduled notifications into the queue. **Singleton.**

Cross-process communication is **only** through Redis (transport/coordination) and Postgres
(state). No direct service-to-service calls. Workers and API scale horizontally; the
scheduler runs as a single replica.

```
                    ┌─────────────┐
   API consumers ──▶│  cmd/api     │  Gin REST + WebSocket + Swagger
                    │  (N replicas)│  validate → persist → enqueue
                    └──────┬───────┘
                           │ writes state          ┌──────────────┐
                           ▼                        │  Postgres     │  source of truth
        ┌──────────────────────────────────────────│  notifications,
        │                  │                        │  batches, templates,
        │ enqueue (Redis)  │ read/write state       │  delivery_attempts
        ▼                  ▼                        └──────────────┘
   ┌──────────┐      ┌──────────────┐
   │  Redis    │◀────│ cmd/scheduler │ promotes due scheduled items → queue
   │ priority  │     │  (1 replica)  │
   │ queues    │     └──────────────┘
   │ rate limit│           ┌──────────────┐
   │ idempotency◀──────────│  cmd/worker   │ dequeue → rate-limit → deliver
   │ retry ZSET│           │  (N replicas) │ → webhook.site → update state
   │ DLQ       │           └──────┬───────┘ → publish status event
   └──────────┘                   │
                                   ▼  status events → WebSocket hub, metrics
                            OTel → Jaeger · /metrics → Prometheus → Grafana
```

### 4.2 Clean Architecture layering
Strict dependency rule: **dependencies point inward only.** The `usecase` layer defines
ports (interfaces); outer adapters implement them; `cmd/*` binaries are the composition root
that injects concrete implementations.

```
cmd/
  api/main.go            # composition root: wire deps → usecases → start Gin + WS
  worker/main.go         # composition root: wire usecases + processing loop
  scheduler/main.go      # composition root: wire scheduler usecase + ticker

internal/
  domain/                # ENTERPRISE RULES — no external deps
                         #   entities: Notification, Batch, Template, DeliveryAttempt
                         #   enums: Channel, Status, Priority
                         #   invariants, validation, domain errors. Pure Go.

  usecase/               # APPLICATION RULES — depends only on domain
                         #   interactors: CreateNotification, CreateBatch,
                         #     ProcessNotification, CancelNotification, ListNotifications,
                         #     PromoteScheduled, RenderTemplate
                         #   PORTS (interfaces): NotificationRepository, BatchRepository,
                         #     TemplateRepository, Queue, RetryQueue, DLQ, RateLimiter,
                         #     IdempotencyStore, Provider, EventPublisher, Clock

  adapter/               # INTERFACE ADAPTERS — implement ports, depend on domain+usecase
    http/                #   Gin controllers, DTOs, middleware (correlation id, recovery, tracing)
    repository/postgres/ #   implements *Repository ports
    queue/redis/         #   implements Queue / RetryQueue / DLQ / scheduled-set ports
    ratelimit/redis/     #   implements RateLimiter (token bucket)
    idempotency/redis/   #   implements IdempotencyStore
    provider/webhook/    #   implements Provider (webhook.site HTTP client)
    ws/                  #   implements EventPublisher (websocket hub fan-out)

  infrastructure/        # FRAMEWORKS & DRIVERS — outermost, no business logic
                         #   db (pgx pool + migrate), redis client, config (env),
                         #   observability (slog, prometheus, otel/jaeger), http server bootstrap

migrations/   deploy/   api/openapi/
```

Each `internal/` package has one responsibility and a narrow interface. Interactors are
unit-tested against fake ports; adapters are integration-tested against real Postgres/Redis.

## 5. Data Model (PostgreSQL)

**notifications**
- `id` uuid PK
- `batch_id` uuid null (FK → batches)
- `channel` enum (sms | email | push)
- `recipient` text
- `content` text
- `template_id` uuid null (FK → templates)
- `variables` jsonb null
- `priority` enum (high | normal | low)
- `status` enum (pending | queued | sending | delivered | failed | cancelled)
- `idempotency_key` text null, **unique**
- `scheduled_at` timestamptz null
- `attempts` int default 0
- `last_error` text null
- `provider_message_id` text null
- `created_at`, `updated_at` timestamptz
- Indexes: `status`, `channel`, `created_at`, `batch_id`; unique on `idempotency_key`.

**batches**
- `id` uuid PK, `total` int, `created_at` timestamptz. Status is derived/aggregated from children.

**delivery_attempts** (full audit trail)
- `id` uuid PK, `notification_id` uuid FK, `attempt_no` int, `status` text,
  `provider_response` jsonb null, `error` text null, `attempted_at` timestamptz.

**templates**
- `id` uuid PK, `name` text, `channel` enum, `body` text, `created_at` timestamptz.

### Status lifecycle
```
pending → queued → sending → delivered
                          ↘ failed   (after max retries → DLQ)
pending/queued/scheduled → cancelled
```
Scheduled items start `pending` with a future `scheduled_at`.

## 6. Core Mechanics

### 6.1 Create (single / batch)
Validate → render template if `template_id` provided → check idempotency key (return existing
on hit) → persist `pending` → if `scheduled_at` is in the future, add to Redis scheduled ZSET;
otherwise enqueue to the priority queue and mark `queued`. Batches (up to 1000) are inserted
in a single transaction.

### 6.2 Priority queue
Three Redis structures (high / normal / low). Workers drain high first, then normal, then low,
with anti-starvation: periodically service lower-priority queues so they are not starved under
sustained high-priority load.

### 6.3 Rate limiting
Redis token-bucket keyed per channel, refilled to **100 tokens/sec/channel**. A worker that
cannot acquire a token **re-queues** the item with a small delay (rather than blocking the
worker or dropping the item).

### 6.4 Idempotency
Client supplies an `Idempotency-Key` (header for single, per-item for batch). Stored unique in
Postgres plus a short-lived Redis cache. A duplicate key returns the original notification with
no second send.

### 6.5 Delivery
Worker sets `sending`, POSTs to webhook.site
(`{ "to", "channel", "content" }`), expects `202` + `{ messageId, status, timestamp }`,
records a `delivery_attempt`, sets `delivered`, and stores `provider_message_id`.

### 6.6 Retry (fixed interval + DLQ)
On failure / timeout / non-202: increment `attempts`, write the attempt row, and if
`attempts < max` (configurable, **default 5**) push to a retry ZSET scored `now + interval`.
A retry poller moves due items back into the queue. At max attempts → DLQ (Redis list +
`status = failed`, `last_error` set).

### 6.7 Cancel
Only `pending` / `queued` / scheduled notifications can be cancelled → `cancelled`, and the
item is removed from the queue / scheduled set. In-flight (`sending`) notifications cannot be
cancelled.

### 6.8 Scheduling
Future-dated notifications live in a Redis scheduled ZSET (score = `scheduled_at`). The
`cmd/scheduler` singleton polls for due items and promotes them into the priority queue.

### 6.9 Templates
Templates store a body with `{{variable}}` placeholders. At create time, a notification
referencing a `template_id` plus a `variables` map renders to final `content`.

## 7. API Surface (Gin + Swagger)

```
POST   /api/v1/notifications              create one
POST   /api/v1/notifications/batch        create up to 1000
GET    /api/v1/notifications/{id}         status by id
GET    /api/v1/notifications              list: filter status,channel,date range + pagination
DELETE /api/v1/notifications/{id}         cancel pending
GET    /api/v1/batches/{id}               batch status summary
POST   /api/v1/templates                  create template
GET    /api/v1/templates                  list templates
GET    /ws/notifications                  websocket status stream (filter by id/batch)
GET    /healthz                           liveness
GET    /readyz                            readiness (Postgres + Redis)
GET    /metrics                           Prometheus
GET    /swagger                           OpenAPI UI
```

## 8. Observability

- **Metrics** (`/metrics`, Prometheus): queue depth per priority & channel; enqueued /
  delivered / failed / retried counters; delivery latency histogram; rate-limit rejections;
  DLQ size; in-flight gauge. Grafana dashboard provisioned via docker-compose.
- **Tracing** (OpenTelemetry → Jaeger): spans linked by correlation id across
  API request → enqueue → worker dequeue → provider call.
- **Logging**: `slog` structured JSON; correlation id propagated via context and the
  `X-Correlation-ID` header (middleware generates one if absent).
- **Health**: `/healthz` (liveness), `/readyz` (checks Postgres + Redis).

## 9. Testing, Linting & Tooling

- **Linting**: `golangci-lint` with committed `.golangci.yml` (govet, staticcheck, errcheck,
  revive, gocritic, gosec, gofumpt, ineffassign, unconvert, ...). Run via `make lint`; must
  pass clean.
- **Formatting**: `gofumpt` / `goimports` via `make fmt`.
- **Unit tests**: interactors + domain validation + template render + retry scheduling +
  priority selection + token-bucket, using fake ports.
- **Integration tests**: Postgres/Redis adapters + provider via testcontainers-go; webhook.site
  mocked with `httptest` (202, error, timeout cases).
- **One-command entry points**: `make test`, `make lint`, `make up` (docker-compose). The
  `Makefile` is the single source of truth for all checks.

## 10. Deliverables

1. Source code: Git repo with clean commit history.
2. `README.md`: setup instructions, architecture overview, API examples.
3. Docker Compose: one-command setup (`docker-compose up`) — api, worker(s), scheduler,
   Postgres, Redis, Prometheus, Grafana, Jaeger.
4. API documentation: Swagger / OpenAPI.
5. Database migrations: versioned SQL.
6. Test suite: runnable with a single command.

## 11. Configuration (env)

Key tunables: HTTP port, Postgres DSN, Redis address, rate limit (per-channel/sec, default 100),
max retry attempts (default 5), retry interval, scheduler poll interval, worker concurrency,
OTel/Jaeger endpoint. All wired through `internal/infrastructure/config`.
