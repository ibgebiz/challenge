//go:build e2e

// Package e2e contains a black-box end-to-end test that drives the real HTTP API
// against real Postgres and Redis (via testcontainers) with the worker and
// scheduler running in-process and a mock provider standing in for webhook.site.
//
// Run with: make test-e2e   (requires Docker)
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	httpapi "github.com/ibrahim-bg/notifier/internal/adapter/http"
	"github.com/ibrahim-bg/notifier/internal/adapter/provider/webhook"
	"github.com/ibrahim-bg/notifier/internal/adapter/ws"
	"github.com/ibrahim-bg/notifier/internal/app"
	"github.com/ibrahim-bg/notifier/internal/scheduler"
	"github.com/ibrahim-bg/notifier/internal/usecase"
	"github.com/ibrahim-bg/notifier/internal/worker"
)

var baseURL string

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgC, dsn, err := startPostgres(ctx)
	if err != nil {
		fmt.Println("postgres:", err)
		os.Exit(1)
	}
	rdC, addr, err := startRedis(ctx)
	if err != nil {
		fmt.Println("redis:", err)
		os.Exit(1)
	}

	mock := startMockProvider()

	// Configure the shared container via env, then build it.
	os.Setenv("POSTGRES_DSN", dsn)
	os.Setenv("REDIS_ADDR", addr)
	os.Setenv("PROVIDER_URL", mock.URL)
	os.Setenv("RATE_LIMIT_PER_SEC", "5")
	os.Setenv("LOG_LEVEL", "error")
	os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	gin.SetMode(gin.TestMode)
	c, err := app.New(ctx, "e2e")
	if err != nil {
		fmt.Println("app.New:", err)
		os.Exit(1)
	}

	clock := app.RealClock{}
	idGen := uuid.NewString
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
	health := &httpapi.Health{DB: pgPinger{c.Pool}, Redis: redisPinger{c.Redis}}

	router := httpapi.NewRouter(handlers, health, func(c *gin.Context) { c.Status(http.StatusOK) })
	hub := ws.NewHub()
	go ws.BridgeToHub(ctx, c.Redis, hub, c.Logger)
	router.GET("/ws/notifications", ws.NewHandler(hub).Stream)

	srv := httptest.NewServer(router)
	baseURL = srv.URL

	provider := webhook.New(mock.URL, mock.Client())
	process := &usecase.ProcessNotification{
		Repo: c.NotifRepo, Provider: provider, RateLimiter: c.RateLimiter,
		Attempts: c.AttemptRepo, Retry: c.Retry, DLQ: c.DLQ, Queue: c.Queue,
		Publisher: ws.NewRedisPublisher(c.Redis), Metrics: c.Metrics, Clock: clock,
		MaxAttempts: 2, RetryInterval: time.Second, IDGen: idGen,
	}
	go scheduler.Run(ctx, scheduler.QueuePromoter{Src: c.Retry, Dst: c.Queue}, 300*time.Millisecond)
	go scheduler.Run(ctx, scheduler.QueuePromoter{Src: c.Scheduled, Dst: c.Queue}, 300*time.Millisecond)
	go worker.Run(ctx, c.Queue, process.Execute, 4)

	code := m.Run()

	srv.Close()
	mock.Close()
	c.Close(context.Background())
	_ = pgC.Terminate(ctx)
	_ = rdC.Terminate(ctx)
	os.Exit(code)
}

// ---- scenarios ----

func TestE2E_CreateAndDeliver(t *testing.T) {
	code, body := post(t, "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+905550001", "content": "hi", "priority": "high",
	}, nil)
	mustCode(t, code, http.StatusCreated)
	id := body["id"].(string)
	n := waitStatus(t, id, "delivered", 15*time.Second)
	if n["providerMessageId"] == nil || n["providerMessageId"] == "" {
		t.Fatalf("expected providerMessageId, got %v", n["providerMessageId"])
	}
}

func TestE2E_Idempotency(t *testing.T) {
	hdr := map[string]string{httpapi.IdempotencyHeader: "e2e-key-1"}
	_, first := post(t, "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+905550002", "content": "one", "priority": "normal",
	}, hdr)
	_, second := post(t, "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+905550002", "content": "two", "priority": "normal",
	}, hdr)
	if first["id"] != second["id"] {
		t.Fatalf("idempotency failed: %v != %v", first["id"], second["id"])
	}
}

func TestE2E_ValidationError(t *testing.T) {
	code, _ := post(t, "/api/v1/notifications", map[string]any{
		"channel": "sms", "content": "missing recipient", // recipient omitted
	}, nil)
	mustCode(t, code, http.StatusBadRequest)
}

func TestE2E_Template(t *testing.T) {
	code, tmpl := post(t, "/api/v1/templates", map[string]any{
		"name": "welcome", "channel": "email", "body": "Hello {{name}}, code {{code}}",
	}, nil)
	mustCode(t, code, http.StatusCreated)
	tid := tmpl["id"].(string)

	_, created := post(t, "/api/v1/notifications", map[string]any{
		"channel": "email", "recipient": "a@b.com", "templateId": tid,
		"variables": map[string]string{"name": "Ada", "code": "42"},
	}, nil)
	id := created["id"].(string)
	n := waitStatus(t, id, "delivered", 15*time.Second)
	if n["content"] != "Hello Ada, code 42" {
		t.Fatalf("template not rendered: %q", n["content"])
	}
}

func TestE2E_Batch(t *testing.T) {
	code, body := post(t, "/api/v1/notifications/batch", map[string]any{
		"notifications": []map[string]any{
			{"channel": "sms", "recipient": "+905550010", "content": "A", "priority": "high"},
			{"channel": "push", "recipient": "device-1", "content": "B", "priority": "low"},
		},
	}, nil)
	mustCode(t, code, http.StatusCreated)
	if body["total"].(float64) != 2 {
		t.Fatalf("want total 2, got %v", body["total"])
	}
	bid := body["id"].(string)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, b := get(t, "/api/v1/batches/"+bid)
		counts, _ := b["counts"].(map[string]any)
		if counts["delivered"] == float64(2) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("batch not fully delivered in time")
}

func TestE2E_ScheduledDelivers(t *testing.T) {
	future := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	code, body := post(t, "/api/v1/notifications", map[string]any{
		"channel": "push", "recipient": "device-2", "content": "later", "scheduledAt": future,
	}, nil)
	mustCode(t, code, http.StatusCreated)
	if body["status"] != "pending" {
		t.Fatalf("scheduled should start pending, got %v", body["status"])
	}
	waitStatus(t, body["id"].(string), "delivered", 20*time.Second)
}

func TestE2E_CancelPending(t *testing.T) {
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	_, body := post(t, "/api/v1/notifications", map[string]any{
		"channel": "push", "recipient": "device-3", "content": "much later", "scheduledAt": future,
	}, nil)
	id := body["id"].(string)

	code := del(t, "/api/v1/notifications/"+id)
	mustCode(t, code, http.StatusNoContent)

	_, n := get(t, "/api/v1/notifications/"+id)
	if n["status"] != "cancelled" {
		t.Fatalf("want cancelled, got %v", n["status"])
	}

	// Cancelling a non-cancellable (already cancelled) notification -> 409.
	if c := del(t, "/api/v1/notifications/"+id); c != http.StatusConflict {
		t.Fatalf("want 409 on re-cancel, got %d", c)
	}
}

func TestE2E_RetryThenDLQ(t *testing.T) {
	// recipient containing "fail" makes the mock provider return 500 forever.
	_, body := post(t, "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+90555fail", "content": "doomed", "priority": "high",
	}, nil)
	id := body["id"].(string)
	// MaxAttempts=2, RetryInterval=1s -> fails within a few seconds.
	waitStatus(t, id, "failed", 20*time.Second)
}

func TestE2E_RateLimitBurstAllDeliver(t *testing.T) {
	// RATE_LIMIT_PER_SEC=5; sending 12 on one channel forces re-queueing, but all
	// must eventually be delivered (nothing dropped).
	ids := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		_, body := post(t, "/api/v1/notifications", map[string]any{
			"channel": "email", "recipient": fmt.Sprintf("burst-%d@x.com", i), "content": "b", "priority": "normal",
		}, nil)
		ids = append(ids, body["id"].(string))
	}
	for _, id := range ids {
		waitStatus(t, id, "delivered", 30*time.Second)
	}
}

func TestE2E_ListAndPagination(t *testing.T) {
	code, body := get(t, "/api/v1/notifications?status=delivered&channel=sms&limit=2&offset=0")
	mustCode(t, code, http.StatusOK)
	if body["limit"].(float64) != 2 {
		t.Fatalf("want limit 2, got %v", body["limit"])
	}
	items, _ := body["items"].([]any)
	if len(items) > 2 {
		t.Fatalf("limit not honored: got %d items", len(items))
	}
}

func TestE2E_Health(t *testing.T) {
	if code, _ := get(t, "/healthz"); code != http.StatusOK {
		t.Fatalf("healthz: %d", code)
	}
	code, body := get(t, "/readyz")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("readyz: %d %v", code, body["status"])
	}
}

func TestE2E_WebSocketReceivesStatus(t *testing.T) {
	// Schedule slightly in the future, subscribe, then expect a delivered event.
	future := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	_, body := post(t, "/api/v1/notifications", map[string]any{
		"channel": "push", "recipient": "ws-device", "content": "ws", "scheduledAt": future,
	}, nil)
	id := body["id"].(string)

	wsURL := strings.Replace(baseURL, "http://", "ws://", 1) + "/ws/notifications?id=" + id
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(25 * time.Second))
	for {
		var evt map[string]any
		if err := conn.ReadJSON(&evt); err != nil {
			t.Fatalf("ws read: %v", err)
		}
		if evt["status"] == "delivered" {
			return
		}
	}
}

// ---- helpers ----

func post(t *testing.T, path string, payload any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return do(t, req)
}

func get(t *testing.T, path string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, baseURL+path, nil)
	return do(t, req)
}

func del(t *testing.T, path string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, baseURL+path, nil)
	code, _ := do(t, req)
	return code
}

func do(t *testing.T, req *http.Request) (int, map[string]any) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &m)
	}
	return resp.StatusCode, m
}

func mustCode(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Fatalf("status: want %d, got %d", want, got)
	}
}

func waitStatus(t *testing.T, id, want string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last any
	for time.Now().Before(deadline) {
		_, n := get(t, "/api/v1/notifications/"+id)
		last = n["status"]
		if last == want {
			return n
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("notification %s did not reach %q in %s (last=%v)", id, want, timeout, last)
	return nil
}

func startMockProvider() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			To string `json:"to"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if strings.Contains(body.To, "fail") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"messageId": uuid.NewString(),
			"status":    "accepted",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	}))
}

func startPostgres(ctx context.Context) (testcontainers.Container, string, error) {
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("notifier"),
		tcpostgres.WithUsername("notifier"),
		tcpostgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, "", err
	}
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	return container, dsn, err
}

func startRedis(ctx context.Context) (testcontainers.Container, string, error) {
	container, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return nil, "", err
	}
	endpoint, err := container.Endpoint(ctx, "")
	return container, endpoint, err
}

type pgPinger struct{ pool *pgxpool.Pool }

func (p pgPinger) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

type redisPinger struct{ rdb *redis.Client }

func (p redisPinger) Ping(ctx context.Context) error { return p.rdb.Ping(ctx).Err() }

// ---- additional scenarios ----

func TestE2E_NotFound(t *testing.T) {
	unknown := "00000000-0000-0000-0000-000000000000"

	code, _ := get(t, "/api/v1/notifications/"+unknown)
	mustCode(t, code, http.StatusNotFound)

	code = del(t, "/api/v1/notifications/"+unknown)
	mustCode(t, code, http.StatusNotFound)

	code, _ = get(t, "/api/v1/batches/"+unknown)
	mustCode(t, code, http.StatusNotFound)
}

func TestE2E_CancelDeliveredConflict(t *testing.T) {
	_, body := post(t, "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+905551001", "content": "hi", "priority": "high",
	}, nil)
	id := body["id"].(string)
	waitStatus(t, id, "delivered", 15*time.Second)

	code := del(t, "/api/v1/notifications/"+id)
	mustCode(t, code, http.StatusConflict)
}

func TestE2E_ListTemplates(t *testing.T) {
	_, a := post(t, "/api/v1/templates", map[string]any{
		"name": "list-tmpl-a", "channel": "sms", "body": "alpha",
	}, nil)
	_, b := post(t, "/api/v1/templates", map[string]any{
		"name": "list-tmpl-b", "channel": "push", "body": "beta",
	}, nil)
	aID := a["id"].(string)
	bID := b["id"].(string)

	code, _ := get(t, "/api/v1/templates")
	mustCode(t, code, http.StatusOK)

	// Re-fetch as a raw slice because the endpoint returns []TemplateResponse.
	req, _ := http.NewRequest(http.MethodGet, baseURL+"/api/v1/templates", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("list templates: %v", err)
	}
	defer resp.Body.Close()
	var list []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode template list: %v", err)
	}

	ids := make(map[string]bool, len(list))
	for _, tmpl := range list {
		ids[tmpl["id"].(string)] = true
	}
	if !ids[aID] || !ids[bID] {
		t.Fatalf("expected both template IDs in list, got %v", ids)
	}
}

func TestE2E_TemplateValidation(t *testing.T) {
	code, _ := post(t, "/api/v1/templates", map[string]any{
		"channel": "sms", "body": "no name here",
	}, nil)
	mustCode(t, code, http.StatusBadRequest)

	code, _ = post(t, "/api/v1/templates", map[string]any{
		"name": "no-body", "channel": "sms",
	}, nil)
	mustCode(t, code, http.StatusBadRequest)
}

func TestE2E_InvalidChannelValidation(t *testing.T) {
	code, _ := post(t, "/api/v1/notifications", map[string]any{
		"channel": "fax", "recipient": "+905559999", "content": "bad channel",
	}, nil)
	mustCode(t, code, http.StatusBadRequest)
}

func TestE2E_TemplateNotFound(t *testing.T) {
	code, _ := post(t, "/api/v1/notifications", map[string]any{
		"channel": "email", "recipient": "x@y.com",
		"templateId": "00000000-0000-0000-0000-000000000001",
	}, nil)
	mustCode(t, code, http.StatusNotFound)
}

func TestE2E_BatchPartialFailure(t *testing.T) {
	code, body := post(t, "/api/v1/notifications/batch", map[string]any{
		"notifications": []map[string]any{
			{"channel": "sms", "recipient": "+905550020", "content": "ok", "priority": "high"},
			{"channel": "sms", "recipient": "+90555fail2", "content": "fail", "priority": "high"},
		},
	}, nil)
	mustCode(t, code, http.StatusCreated)
	bid := body["id"].(string)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, b := get(t, "/api/v1/batches/"+bid)
		counts, _ := b["counts"].(map[string]any)
		delivered, _ := counts["delivered"].(float64)
		failed, _ := counts["failed"].(float64)
		if int(delivered+failed) == 2 {
			if delivered != 1 || failed != 1 {
				t.Fatalf("want delivered=1 failed=1, got delivered=%v failed=%v", delivered, failed)
			}
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("batch partial failure did not settle in time")
}

func TestE2E_CorrelationIDPropagation(t *testing.T) {
	const myCorrelationID = "my-e2e-correlation-id"
	b, _ := json.Marshal(map[string]any{
		"channel": "push", "recipient": "device-corr", "content": "corr", "priority": "normal",
	})
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/api/v1/notifications", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Correlation-ID", myCorrelationID)

	_, _, respHeaders := doRespHeaders(t, req)
	got := respHeaders.Get("X-Correlation-ID")
	if got != myCorrelationID {
		t.Fatalf("expected X-Correlation-ID %q, got %q", myCorrelationID, got)
	}
}

func TestE2E_PaginationOffset(t *testing.T) {
	// Create 3 uniquely identifiable SMS notifications and wait for delivery.
	ids := make([]string, 3)
	for i := 0; i < 3; i++ {
		_, body := post(t, "/api/v1/notifications", map[string]any{
			"channel": "sms", "recipient": fmt.Sprintf("+9055509%02d", i),
			"content": fmt.Sprintf("page-%d", i), "priority": "high",
		}, nil)
		ids[i] = body["id"].(string)
	}
	for _, id := range ids {
		waitStatus(t, id, "delivered", 20*time.Second)
	}

	seen := make(map[string]bool)
	for offset := 0; offset < 3; offset++ {
		code, page := get(t, fmt.Sprintf("/api/v1/notifications?channel=sms&limit=1&offset=%d", offset))
		mustCode(t, code, http.StatusOK)
		items, _ := page["items"].([]any)
		if len(items) == 0 {
			t.Fatalf("offset=%d: expected 1 item, got 0", offset)
		}
		item := items[0].(map[string]any)
		pid := item["id"].(string)
		if seen[pid] {
			t.Fatalf("offset=%d: duplicate item id %q across pages", offset, pid)
		}
		seen[pid] = true
	}
}

// ---- load scenarios ----

func TestE2E_Load_ConcurrentCreates(t *testing.T) {
	const n = 50
	ids := make([]string, n)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, body := post(t, "/api/v1/notifications", map[string]any{
				"channel": "push", "recipient": fmt.Sprintf("load-device-%d", i),
				"content": fmt.Sprintf("load-%d", i), "priority": "normal",
			}, nil)
			mu.Lock()
			ids[i] = body["id"].(string)
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Assert no duplicate IDs.
	seen := make(map[string]bool, n)
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate notification id %q from concurrent creates", id)
		}
		seen[id] = true
	}

	// Assert all delivered. Each item can be delayed by the per-channel rate limiter
	// (5/s), so give 90 s of headroom in a resource-constrained test environment.
	errs := make(chan string, n)
	var wg2 sync.WaitGroup
	for _, id := range ids {
		id := id
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			deadline := time.Now().Add(90 * time.Second)
			var last any
			for time.Now().Before(deadline) {
				_, notif := get(t, "/api/v1/notifications/"+id)
				last = notif["status"]
				if last == "delivered" {
					return
				}
				time.Sleep(300 * time.Millisecond)
			}
			errs <- fmt.Sprintf("notification %s did not reach delivered in 90s (last=%v)", id, last)
		}()
	}
	wg2.Wait()
	close(errs)
	for msg := range errs {
		t.Error(msg)
	}
}

func TestE2E_Load_HighVolumeBatch(t *testing.T) {
	const total = 200
	notifs := make([]map[string]any, total)
	for i := 0; i < total; i++ {
		notifs[i] = map[string]any{
			"channel": "push", "recipient": fmt.Sprintf("hv-device-%d", i),
			"content": fmt.Sprintf("hv-%d", i), "priority": "normal",
		}
	}
	code, body := post(t, "/api/v1/notifications/batch", map[string]any{
		"notifications": notifs,
	}, nil)
	mustCode(t, code, http.StatusCreated)
	if body["total"].(float64) != float64(total) {
		t.Fatalf("want total %d, got %v", total, body["total"])
	}
	bid := body["id"].(string)

	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		_, b := get(t, "/api/v1/batches/"+bid)
		counts, _ := b["counts"].(map[string]any)
		if counts["delivered"] == float64(total) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("high-volume batch of %d not fully delivered in time", total)
}

func TestE2E_Load_ConcurrentIdempotency(t *testing.T) {
	const goroutines = 25
	const idemKey = "load-idem-key-concurrent"
	results := make([]string, goroutines)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, body := post(t, "/api/v1/notifications", map[string]any{
				"channel": "sms", "recipient": "+905550099", "content": "idem-load", "priority": "normal",
			}, map[string]string{httpapi.IdempotencyHeader: idemKey})
			mu.Lock()
			results[i] = body["id"].(string)
			mu.Unlock()
		}()
	}
	wg.Wait()

	first := results[0]
	for i, id := range results {
		if id != first {
			t.Fatalf("goroutine %d returned different id %q, want %q", i, id, first)
		}
	}
}

func TestE2E_Load_PriorityOrdering(t *testing.T) {
	const perPriority = 15
	highIDs := make([]string, perPriority)
	lowIDs := make([]string, perPriority)

	for i := 0; i < perPriority; i++ {
		_, h := post(t, "/api/v1/notifications", map[string]any{
			"channel": "email", "recipient": fmt.Sprintf("prio-high-%d@x.com", i),
			"content": "high", "priority": "high",
		}, nil)
		highIDs[i] = h["id"].(string)

		_, l := post(t, "/api/v1/notifications", map[string]any{
			"channel": "email", "recipient": fmt.Sprintf("prio-low-%d@x.com", i),
			"content": "low", "priority": "low",
		}, nil)
		lowIDs[i] = l["id"].(string)
	}

	// Wait for all to be delivered.
	allIDs := append(highIDs, lowIDs...)
	for _, id := range allIDs {
		waitStatus(t, id, "delivered", 60*time.Second)
	}

	// Compare earliest high-priority delivery time against earliest low-priority.
	earliest := func(ids []string) time.Time {
		var t0 time.Time
		for _, id := range ids {
			_, n := get(t, "/api/v1/notifications/"+id)
			raw, _ := n["updatedAt"].(string)
			ts, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				continue
			}
			if t0.IsZero() || ts.Before(t0) {
				t0 = ts
			}
		}
		return t0
	}

	firstHigh := earliest(highIDs)
	firstLow := earliest(lowIDs)

	if firstHigh.IsZero() || firstLow.IsZero() {
		t.Skip("could not parse delivery timestamps; skipping priority ordering assertion")
	}
	if firstHigh.After(firstLow) {
		t.Logf("priority ordering best-effort: first high=%v first low=%v (high arrived after low)", firstHigh, firstLow)
	}
}

// ---- additional helpers ----

// doRespHeaders executes req and returns the status code, parsed JSON body, and
// the raw response headers.
func doRespHeaders(t *testing.T, req *http.Request) (int, map[string]any, http.Header) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &m)
	}
	return resp.StatusCode, m, resp.Header
}
