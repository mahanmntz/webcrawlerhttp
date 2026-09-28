package robots

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"crawler-engine/internal/testredis"
)

func newServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/robots.txt" {
			hits.Add(1)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(ts.Close)
	return ts, &hits
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestCheck_RulesAndCrawlDelay(t *testing.T) {
	ts, hits := newServer(t, 200, "User-agent: SpiderRAG\nDisallow: /private\nCrawl-delay: 3\n\nUser-agent: *\nDisallow: /\n")
	c := New(testredis.Client(t, 12), ts.Client(), "Test/1.0", "SpiderRAG")
	ctx := context.Background()

	allowed, err := c.Check(ctx, mustURL(t, ts.URL+"/public/page"))
	if err != nil || !allowed.Allowed || allowed.CrawlDelay != 3*time.Second {
		t.Fatalf("public page: %+v err=%v", allowed, err)
	}

	blocked, err := c.Check(ctx, mustURL(t, ts.URL+"/private/data?x=1"))
	if err != nil || blocked.Allowed {
		t.Fatalf("private page should be disallowed: %+v err=%v", blocked, err)
	}

	if hits.Load() != 1 {
		t.Fatalf("robots.txt should be fetched once and cached, got %d fetches", hits.Load())
	}
}

func TestCheck_SharedRedisCacheAcrossInstances(t *testing.T) {
	ts, hits := newServer(t, 200, "User-agent: *\nDisallow: /nope\n")
	rdb := testredis.Client(t, 12)
	ctx := context.Background()

	if _, err := New(rdb, ts.Client(), "Test/1.0", "SpiderRAG").Check(ctx, mustURL(t, ts.URL+"/")); err != nil {
		t.Fatal(err)
	}
	// A second crawler process reuses the policy cached in Redis.
	d, err := New(rdb, ts.Client(), "Test/1.0", "SpiderRAG").Check(ctx, mustURL(t, ts.URL+"/nope"))
	if err != nil || d.Allowed {
		t.Fatalf("expected cached disallow, got %+v err=%v", d, err)
	}
	if hits.Load() != 1 {
		t.Fatalf("expected one fetch across instances, got %d", hits.Load())
	}
}

func TestCheck_StatusSemantics(t *testing.T) {
	ctx := context.Background()

	missing, _ := newServer(t, 404, "")
	d, err := New(testredis.Client(t, 12), missing.Client(), "Test/1.0", "SpiderRAG").Check(ctx, mustURL(t, missing.URL+"/any"))
	if err != nil || !d.Allowed {
		t.Fatalf("404 robots.txt should allow everything: %+v err=%v", d, err)
	}

	broken, _ := newServer(t, 503, "")
	d, err = New(testredis.Client(t, 12), broken.Client(), "Test/1.0", "SpiderRAG").Check(ctx, mustURL(t, broken.URL+"/any"))
	if err != nil || d.Allowed {
		t.Fatalf("5xx robots.txt should disallow everything: %+v err=%v", d, err)
	}
}
