package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crawler-engine/internal/config"
	"crawler-engine/internal/fetcher"
	"crawler-engine/internal/frontier"
	"crawler-engine/internal/models"
	"crawler-engine/internal/politeness"
	"crawler-engine/internal/robots"
	"crawler-engine/internal/testredis"

	"github.com/redis/go-redis/v9"
)

type harness struct {
	t        *testing.T
	rdb      *redis.Client
	pool     *Pool
	frontier *frontier.RedisFrontier
	server   *httptest.Server
	hits     atomic.Int32
}

// newHarness serves robots.txt from robotsBody and every other path via handler.
func newHarness(t *testing.T, robotsBody string, handler http.HandlerFunc) *harness {
	t.Helper()
	h := &harness{t: t, rdb: testredis.Client(t, 14)}

	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			_, _ = w.Write([]byte(robotsBody))
			return
		}
		h.hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(h.server.Close)

	cfg := &config.Config{FetchTimeout: 2 * time.Second, UserAgent: "Test/1.0", AllowPrivateNetworks: true}
	f := fetcher.New(cfg)
	h.frontier = frontier.New(h.rdb, time.Minute, 3)
	h.pool = New(
		Options{WorkerCount: 1, MaxAttempts: 3, MaxCrawlDelay: 30 * time.Second},
		h.frontier, f, politeness.New(h.rdb, 1000),
		robots.New(h.rdb, f.Client(), f.UserAgent(), "SpiderRAG"),
	)
	return h
}

func (h *harness) host() string {
	u, _ := url.Parse(h.server.URL)
	return u.Hostname()
}

// run enqueues a target, dequeues it like a worker would, and handles it.
func (h *harness) run(target models.CrawlTarget) {
	h.t.Helper()
	ctx := context.Background()
	payload, _ := json.Marshal(target)
	if err := h.rdb.LPush(ctx, frontier.QueueFrontierPending, payload).Err(); err != nil {
		h.t.Fatal(err)
	}
	got, raw, err := h.frontier.DequeueReliable(ctx, time.Second)
	if err != nil || got == nil {
		h.t.Fatalf("dequeue: %v", err)
	}
	h.pool.Handle(ctx, "test-worker", got, raw)

	if n, _ := h.rdb.LLen(ctx, frontier.QueueFrontierProcessing).Result(); n != 0 {
		h.t.Fatalf("target was not settled: %d left in processing", n)
	}
}

func (h *harness) count(key string) int64 {
	n, _ := h.rdb.LLen(context.Background(), key).Result()
	return n
}

func (h *harness) jobCounter(field string) string {
	v, _ := h.rdb.HGet(context.Background(), "job:job-1", field).Result()
	return v
}

func (h *harness) delayed() []models.CrawlTarget {
	items, _ := h.rdb.ZRange(context.Background(), frontier.FrontierDelayed, 0, -1).Result()
	var out []models.CrawlTarget
	for _, item := range items {
		var target models.CrawlTarget
		_ = json.Unmarshal([]byte(item), &target)
		out = append(out, target)
	}
	return out
}

func (h *harness) target(path string) models.CrawlTarget {
	return models.CrawlTarget{
		JobID: "job-1", URL: h.server.URL + path, MaxDepth: 1, Priority: 5,
		StayInDomain: true, ScopeHost: h.host(), CreatedAt: time.Now(),
	}
}

func htmlPage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte("<html><body><h1>hi</h1></body></html>"))
}

func TestHandle_SuccessPushesRawPage(t *testing.T) {
	h := newHarness(t, "", htmlPage)
	h.run(h.target("/page"))

	if h.count(frontier.QueueRawPages) != 1 {
		t.Fatal("expected RawPage to be pushed to the parser queue")
	}
	raw, _ := h.rdb.LIndex(context.Background(), frontier.QueueRawPages, 0).Result()
	var page models.RawPage
	_ = json.Unmarshal([]byte(raw), &page)
	if page.ScopeHost != h.host() {
		t.Fatalf("scope_host not propagated to RawPage: %q", page.ScopeHost)
	}
	if h.jobCounter(counterFetched) != "1" {
		t.Fatal("expected pages_fetched counter")
	}
}

func TestHandle_ServerErrorIsRetriedWithBackoff(t *testing.T) {
	h := newHarness(t, "", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	h.run(h.target("/flaky"))

	delayed := h.delayed()
	if len(delayed) != 1 || delayed[0].Attempts != 1 {
		t.Fatalf("expected one delayed retry with attempts=1, got %+v", delayed)
	}
	if h.count(frontier.QueueRawPages) != 0 || h.count(frontier.FrontierDead) != 0 {
		t.Fatal("a retryable failure must not be pushed or dead-lettered")
	}
}

func TestHandle_LastAttemptIsDeadLettered(t *testing.T) {
	h := newHarness(t, "", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) })
	target := h.target("/down")
	target.Attempts = 2 // MaxAttempts is 3
	h.run(target)

	if h.count(frontier.FrontierDead) != 1 || len(h.delayed()) != 0 {
		t.Fatal("expected exhausted target in the dead-letter list")
	}
	raw, _ := h.rdb.LIndex(context.Background(), frontier.FrontierDead, 0).Result()
	if !strings.Contains(raw, "HTTP 502") {
		t.Fatalf("dead letter should record the reason, got %s", raw)
	}
}

func TestHandle_ClientErrorIsNotRetried(t *testing.T) {
	h := newHarness(t, "", http.NotFound)
	h.run(h.target("/missing"))

	if len(h.delayed()) != 0 || h.count(frontier.QueueRawPages) != 0 {
		t.Fatal("404 must be dropped, not retried or parsed")
	}
	if h.jobCounter(counterFailed) != "1" {
		t.Fatal("expected pages_failed counter")
	}
}

func TestHandle_NonHTMLIsSkipped(t *testing.T) {
	h := newHarness(t, "", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("%PDF-1.7"))
	})
	h.run(h.target("/report"))

	if h.count(frontier.QueueRawPages) != 0 || h.jobCounter(counterSkipped) != "1" {
		t.Fatal("non-HTML responses must be skipped")
	}
}

func TestHandle_RobotsDisallowSkipsWithoutFetching(t *testing.T) {
	h := newHarness(t, "User-agent: *\nDisallow: /private\n", htmlPage)
	h.run(h.target("/private/page"))

	if h.hits.Load() != 0 {
		t.Fatal("disallowed URL must not be fetched")
	}
	if h.jobCounter(counterSkipped) != "1" {
		t.Fatal("expected pages_skipped counter")
	}
}

func TestHandle_BusyHostIsParkedNotFetched(t *testing.T) {
	h := newHarness(t, "", htmlPage)
	h.run(h.target("/first"))
	h.run(h.target("/second"))

	if h.hits.Load() != 1 {
		t.Fatalf("second target must wait for the politeness window, got %d fetches", h.hits.Load())
	}
	if delayed := h.delayed(); len(delayed) != 1 || delayed[0].Attempts != 0 {
		t.Fatalf("expected the second target parked without using an attempt, got %+v", delayed)
	}
}

func TestHandle_InvalidURLIsDeadLetteredNotLooped(t *testing.T) {
	h := newHarness(t, "", htmlPage)
	target := h.target("")
	target.URL = "not-a-url"
	h.run(target)

	if h.count(frontier.FrontierDead) != 1 || h.count(frontier.QueueFrontierPending) != 0 || len(h.delayed()) != 0 {
		t.Fatal("invalid URL must be dead-lettered once, not requeued")
	}
}

func TestHandle_RedirectOutOfScopeIsSkipped(t *testing.T) {
	h := newHarness(t, "", nil)
	h.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			return
		}
		if r.Host == "localhost" || strings.HasPrefix(r.Host, "localhost:") {
			htmlPage(w, r)
			return
		}
		// Same server, different host name: out of scope for 127.0.0.1.
		http.Redirect(w, r, strings.Replace(h.server.URL, "127.0.0.1", "localhost", 1)+"/elsewhere", http.StatusFound)
	})
	h.run(h.target("/moved"))

	if h.count(frontier.QueueRawPages) != 0 || h.jobCounter(counterSkipped) != "1" {
		t.Fatal("page reached via an out-of-scope redirect must be skipped")
	}
}

func TestHandle_SlowNetworkDoesNotStrandTheTarget(t *testing.T) {
	// Regression: the settling context used to be created before the robots
	// and page fetches, so a slow network call exhausted it and the retry
	// could not be written, leaving the target stuck in processing.
	h := newHarness(t, "", nil)
	h.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			http.NotFound(w, r)
			return
		}
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	h.pool.bookkeepingTimeout = 100 * time.Millisecond
	h.run(h.target("/slow"))

	if delayed := h.delayed(); len(delayed) != 1 || delayed[0].Attempts != 1 {
		t.Fatalf("expected the retry to be scheduled despite the slow fetch, got %+v", delayed)
	}
}
