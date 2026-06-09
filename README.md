# Event-Driven Notification System

A scalable notification system that accepts requests over a REST API, processes them
asynchronously through Redis-backed priority queues, and delivers them to an external
provider (simulated with [webhook.site](https://webhook.site)) across **SMS / Email / Push**
channels — with priority queueing, per-channel rate limiting, idempotency, retries with a
dead-letter queue, scheduled delivery, message templates, real-time WebSocket status
updates, and full observability (Prometheus, Grafana, OpenTelemetry/Jaeger).

Built in **Go** following **Clean Architecture**.

---

## Architecture

Three runnable binaries share one `internal/` core and communicate only through **Postgres**
(system of record) and **Redis** (queue/coordination). Workers and the API scale horizontally;
the scheduler runs as a singleton.

```
                    ┌─────────────┐
   API consumers ──▶│  cmd/api     │  Gin REST + WebSocket + Swagger + /metrics
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
   │ DLQ       │           └──────┬───────┘ → publish status event (Redis pub/sub)
   └──────────┘                   │
                                  ▼  status events → WebSocket hub, metrics
                            OTel → Jaeger · /metrics → Prometheus → Grafana
```

### Clean Architecture layers (dependencies point inward only)

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Entities | `internal/domain` | Entities, enums, validation, template rendering. No external deps. |
| Application | `internal/usecase` | Interactors + **ports** (interfaces): `Create/Process/Cancel/...` |
| Adapters | `internal/adapter/*` | Implement ports: `http` (Gin), `repository/postgres`, `queue/redis`, `ratelimit/redis`, `idempotency/redis`, `provider/webhook`, `ws` |
| Frameworks | `internal/infrastructure/*` | db, redis client, config, observability |
| Composition | `internal/app`, `cmd/*` | Build infra/adapters, inject into interactors, run |

---

## Quick start

```bash
cp .env.example .env          # then set PROVIDER_URL (see below)
make up                       # docker compose: api, worker, scheduler, postgres, redis,
                              # prometheus, grafana, jaeger — migrations run on api startup
```

| Service | URL |
|---------|-----|
| API | http://localhost:8080 |
| Swagger UI | http://localhost:8080/swagger/index.html |
| Health | http://localhost:8080/healthz · http://localhost:8080/readyz |
| Metrics | http://localhost:8080/metrics |
| Prometheus | http://localhost:9090 |
| Grafana (anon admin) | http://localhost:3000 → dashboard "Notification System" |
| Jaeger | http://localhost:16686 |

Scale workers: `docker compose -f deploy/docker-compose.yml up --scale worker=3`.

### Configuring the provider (webhook.site)

1. Open https://webhook.site and copy your unique URL (`https://webhook.site/<uuid>`).
2. Set it as `PROVIDER_URL` in `.env` (compose reads it) — e.g. `PROVIDER_URL=https://webhook.site/<uuid>`.
3. On webhook.site, set the default response to **status `202`** with JSON body:
   ```json
   { "messageId": "uuid-here", "status": "accepted", "timestamp": "2026-06-09T00:00:00Z" }
   ```

---

## API examples

Create a single notification (priority high), with an idempotency key:

```bash
curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Your code is 1234","priority":"high"}'
```

Batch create (up to 1000):

```bash
curl -s -X POST localhost:8080/api/v1/notifications/batch \
  -H 'Content-Type: application/json' \
  -d '{"notifications":[
        {"channel":"sms","recipient":"+905551111111","content":"A","priority":"normal"},
        {"channel":"email","recipient":"a@b.com","content":"B","priority":"low"}
      ]}'
```

Schedule for the future:

```bash
curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"push","recipient":"device-1","content":"Reminder","scheduledAt":"2030-01-01T00:00:00Z"}'
```

Templates:

```bash
curl -s -X POST localhost:8080/api/v1/templates \
  -H 'Content-Type: application/json' \
  -d '{"name":"welcome","channel":"email","body":"Hello {{name}}!"}'

curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"email","recipient":"a@b.com","templateId":"<template-id>","variables":{"name":"Ada"}}'
```

Query / list / cancel / batch status:

```bash
curl -s localhost:8080/api/v1/notifications/<id>
curl -s "localhost:8080/api/v1/notifications?status=delivered&channel=sms&limit=20&offset=0"
curl -s -X DELETE localhost:8080/api/v1/notifications/<id>      # only pending/queued
curl -s localhost:8080/api/v1/batches/<batch-id>
```

Real-time status over WebSocket (filter by `id` or `batch`):

```bash
# e.g. with websocat
websocat "ws://localhost:8080/ws/notifications?id=<id>"
```

---

## Design notes

- **Priority queue** — three Redis lists drained high→normal→low via a single `BRPOP`; lower
  priorities are served whenever higher queues are empty (no hard starvation).
- **Rate limiting** — per-channel fixed-window counter in Redis, default **100 msg/s/channel**.
  When no token is available the item is re-queued with a short delay so workers stay free.
- **Idempotency** — `Idempotency-Key` header (single) or per-item key; enforced by a Redis
  `SETNX` claim plus a `UNIQUE` constraint in Postgres (the ultimate source of truth).
- **Retry + DLQ** — fixed-interval retries (default `RETRY_INTERVAL`, max `MAX_RETRY_ATTEMPTS`)
  via a Redis sorted set promoted back into the queue; exhausted items go to the DLQ and are
  marked `failed`. Every attempt is recorded in `delivery_attempts`.
- **Scheduling** — future-dated notifications live in a Redis sorted set; the scheduler
  promotes due items into the queue.
- **Status lifecycle** — `pending → queued → sending → delivered | failed | cancelled`.
- **Observability** — Prometheus metrics (queue depth, delivered/failed/retried, latency,
  DLQ size, rate-limited), structured JSON logs with `X-Correlation-ID`, OpenTelemetry traces
  to Jaeger, and `/healthz` + `/readyz`. The API exposes system-wide queue/DLQ gauges; the
  worker exposes delivery counters on `:8081/metrics`.

---

## Development

```bash
make build              # build all three binaries into bin/
make test               # unit tests (no external services required)
make test-integration   # integration tests (requires Docker; uses testcontainers)
make lint               # golangci-lint
make fmt                # gofumpt + goimports
make swagger            # regenerate OpenAPI docs from handler annotations
```

### Testing strategy

- **Unit tests** — domain validation/rendering and every interactor are tested against
  in-memory fake ports (no Docker). The HTTP layer is tested with Gin + `httptest`; the
  provider with `httptest`.
- **Integration tests** (`-tags=integration`) — Postgres and Redis adapters run against
  ephemeral containers via `testcontainers-go`.

---

## Project layout

```
cmd/{api,worker,scheduler}     # composition roots
internal/
  domain/                      # entities, enums, validation, templates
  usecase/                     # interactors + ports
  adapter/
    http/  ws/                 # Gin handlers, middleware; WebSocket hub + Redis bridge
    repository/postgres/       # repositories
    queue/redis/               # priority queue, retry, DLQ, scheduled store
    ratelimit/redis/  idempotency/redis/  provider/webhook/
  infrastructure/              # config, db, redisclient, observability
  app/                         # shared container wiring
migrations/                    # versioned SQL (golang-migrate, embedded)
deploy/                        # docker-compose, prometheus, grafana
api/openapi/                   # generated Swagger
```

> **Note:** GitHub Actions CI/CD was intentionally left out of scope; linting and tests run
> locally via `make lint test`. A workflow calling `make lint test` can be added in ~15 lines.
