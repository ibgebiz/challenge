package usecase

import (
	"context"
	"sync"
	"time"

	"github.com/ibrahim-bg/notifier/internal/domain"
)

// fixedClock returns a constant time.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// ---- notification repo ----

type fakeNotifRepo struct {
	mu    sync.Mutex
	items map[string]domain.Notification
	byKey map[string]string
}

func newFakeRepo() *fakeNotifRepo {
	return &fakeNotifRepo{items: map[string]domain.Notification{}, byKey: map[string]string{}}
}

func (r *fakeNotifRepo) Create(_ context.Context, n domain.Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[n.ID] = n
	if n.IdempotencyKey != nil {
		r.byKey[*n.IdempotencyKey] = n.ID
	}
	return nil
}

func (r *fakeNotifRepo) CreateBatch(ctx context.Context, _ domain.Batch, ns []domain.Notification) error {
	for _, n := range ns {
		_ = r.Create(ctx, n)
	}
	return nil
}

func (r *fakeNotifRepo) Get(_ context.Context, id string) (domain.Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.items[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return n, nil
}

func (r *fakeNotifRepo) GetByIdempotencyKey(_ context.Context, key string) (domain.Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.byKey[key]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return r.items[id], nil
}

func (r *fakeNotifRepo) UpdateStatus(_ context.Context, id string, s domain.Status, le, pmid *string, attempts int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.items[id]
	n.Status = s
	n.LastError = le
	if pmid != nil {
		n.ProviderMessageID = pmid
	}
	n.Attempts = attempts
	r.items[id] = n
	return nil
}

func (r *fakeNotifRepo) List(_ context.Context, _ NotificationFilter) ([]domain.Notification, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.Notification, 0, len(r.items))
	for _, n := range r.items {
		out = append(out, n)
	}
	return out, len(out), nil
}

// ---- template repo ----

type fakeTemplateRepo struct {
	mu    sync.Mutex
	items map[string]domain.Template
}

func newFakeTemplateRepo() *fakeTemplateRepo {
	return &fakeTemplateRepo{items: map[string]domain.Template{}}
}

func (r *fakeTemplateRepo) Create(_ context.Context, t domain.Template) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[t.ID] = t
	return nil
}

func (r *fakeTemplateRepo) Get(_ context.Context, id string) (domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.items[id]
	if !ok {
		return domain.Template{}, domain.ErrNotFound
	}
	return t, nil
}

func (r *fakeTemplateRepo) List(_ context.Context) ([]domain.Template, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.Template, 0, len(r.items))
	for _, t := range r.items {
		out = append(out, t)
	}
	return out, nil
}

// ---- batch repo ----

type fakeBatchRepo struct {
	mu     sync.Mutex
	items  map[string]domain.Batch
	counts map[string]map[domain.Status]int
}

func newFakeBatchRepo() *fakeBatchRepo {
	return &fakeBatchRepo{items: map[string]domain.Batch{}, counts: map[string]map[domain.Status]int{}}
}

func (r *fakeBatchRepo) Create(_ context.Context, b domain.Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[b.ID] = b
	return nil
}

func (r *fakeBatchRepo) Get(_ context.Context, id string) (domain.Batch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.items[id]
	if !ok {
		return domain.Batch{}, domain.ErrNotFound
	}
	return b, nil
}

func (r *fakeBatchRepo) StatusCounts(_ context.Context, batchID string) (map[domain.Status]int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[batchID], nil
}

// ---- queue ----

type fakeQueue struct {
	mu    sync.Mutex
	lists map[domain.Priority][]string
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{lists: map[domain.Priority][]string{}}
}

func (q *fakeQueue) Enqueue(_ context.Context, item QueueItem) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lists[item.Priority] = append(q.lists[item.Priority], item.NotificationID)
	return nil
}

func (q *fakeQueue) Dequeue(_ context.Context, _ time.Duration) (QueueItem, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, p := range []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow} {
		if len(q.lists[p]) > 0 {
			id := q.lists[p][0]
			q.lists[p] = q.lists[p][1:]
			return QueueItem{NotificationID: id, Priority: p}, true, nil
		}
	}
	return QueueItem{}, false, nil
}

func (q *fakeQueue) Remove(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for p, ids := range q.lists {
		out := ids[:0]
		for _, v := range ids {
			if v != id {
				out = append(out, v)
			}
		}
		q.lists[p] = out
	}
	return nil
}

func (q *fakeQueue) Depth(_ context.Context, p domain.Priority) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.lists[p])), nil
}

func (q *fakeQueue) len(p domain.Priority) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.lists[p]), nil
}

// ---- scheduled store ----

type fakeScheduled struct {
	mu    sync.Mutex
	items map[string]QueueItem
}

func newFakeScheduled() *fakeScheduled {
	return &fakeScheduled{items: map[string]QueueItem{}}
}

func (s *fakeScheduled) Add(_ context.Context, item QueueItem, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[item.NotificationID] = item
	return nil
}

func (s *fakeScheduled) Remove(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
	return nil
}

func (s *fakeScheduled) DuePromote(ctx context.Context, _ time.Time, dst Queue) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for id, item := range s.items {
		_ = dst.Enqueue(ctx, item)
		delete(s.items, id)
		n++
	}
	return n, nil
}

// ---- idempotency ----

type fakeIdem struct {
	mu   sync.Mutex
	seen map[string]string
}

func newFakeIdem() *fakeIdem { return &fakeIdem{seen: map[string]string{}} }

func (i *fakeIdem) Remember(_ context.Context, key, id string) (string, bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if existing, ok := i.seen[key]; ok {
		return existing, true, nil
	}
	i.seen[key] = id
	return "", false, nil
}

// ---- rate limiter ----

type fakeRateLimiter struct{ allow bool }

func (l *fakeRateLimiter) Allow(_ context.Context, _ domain.Channel) (bool, error) {
	return l.allow, nil
}

// ---- provider ----

type fakeProvider struct {
	resp ProviderResponse
	err  error
}

func (p *fakeProvider) Send(_ context.Context, _ domain.Notification) (ProviderResponse, error) {
	return p.resp, p.err
}

// ---- delivery attempts ----

type fakeAttempts struct {
	mu    sync.Mutex
	items []domain.DeliveryAttempt
}

func newFakeAttempts() *fakeAttempts { return &fakeAttempts{} }

func (a *fakeAttempts) Add(_ context.Context, att domain.DeliveryAttempt) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.items = append(a.items, att)
	return nil
}

// ---- retry queue ----

type fakeRetry struct {
	mu    sync.Mutex
	items []QueueItem
}

func newFakeRetry() *fakeRetry { return &fakeRetry{} }

func (r *fakeRetry) Schedule(_ context.Context, item QueueItem, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, item)
	return nil
}

func (r *fakeRetry) DuePromote(ctx context.Context, _ time.Time, dst Queue) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.items)
	for _, item := range r.items {
		_ = dst.Enqueue(ctx, item)
	}
	r.items = nil
	return n, nil
}

func (r *fakeRetry) Size(_ context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.items)), nil
}

// ---- dlq ----

type fakeDLQ struct {
	mu    sync.Mutex
	items []string
}

func newFakeDLQ() *fakeDLQ { return &fakeDLQ{} }

func (d *fakeDLQ) Push(_ context.Context, id, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.items = append(d.items, id)
	return nil
}

func (d *fakeDLQ) Size(_ context.Context) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(len(d.items)), nil
}

// ---- event publisher ----

type fakePublisher struct {
	mu     sync.Mutex
	events []StatusEvent
}

func newFakePublisher() *fakePublisher { return &fakePublisher{} }

func (p *fakePublisher) Publish(_ context.Context, e StatusEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}

// Compile-time assertions that fakes satisfy the ports.
var (
	_ NotificationRepository    = (*fakeNotifRepo)(nil)
	_ BatchRepository           = (*fakeBatchRepo)(nil)
	_ TemplateRepository        = (*fakeTemplateRepo)(nil)
	_ Queue                     = (*fakeQueue)(nil)
	_ ScheduledStore            = (*fakeScheduled)(nil)
	_ IdempotencyStore          = (*fakeIdem)(nil)
	_ RateLimiter               = (*fakeRateLimiter)(nil)
	_ Provider                  = (*fakeProvider)(nil)
	_ DeliveryAttemptRepository = (*fakeAttempts)(nil)
	_ RetryQueue                = (*fakeRetry)(nil)
	_ DLQ                       = (*fakeDLQ)(nil)
	_ EventPublisher            = (*fakePublisher)(nil)
	_ Clock                     = fixedClock{}
)
