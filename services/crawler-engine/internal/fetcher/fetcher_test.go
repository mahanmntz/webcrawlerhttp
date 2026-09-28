package fetcher

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"crawler-engine/internal/config"
	"crawler-engine/internal/models"
)

func testConfig() *config.Config {
	return &config.Config{
		FetchTimeout:         2 * time.Second,
		UserAgent:            "TestCrawler/1.0",
		AllowPrivateNetworks: true, // httptest listens on 127.0.0.1
	}
}

func TestFetcher_FetchSuccess(t *testing.T) {
	// Create mock HTTP server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html><head><title>Test Page</title></head><body><h1>Hello Crawler</h1></body></html>"))
	}))
	defer ts.Close()

	f := New(testConfig())

	target := &models.CrawlTarget{
		JobID:     "test-job-123",
		URL:       ts.URL,
		Depth:     1,
		MaxDepth:  3,
		Priority:  5,
		ScopeHost: "127.0.0.1",
		CreatedAt: time.Now(),
	}

	ctx := context.Background()
	result, err := f.Fetch(ctx, target)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	rawPage := result.Page

	if rawPage.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", rawPage.StatusCode)
	}

	if rawPage.JobID != "test-job-123" {
		t.Errorf("expected job_id 'test-job-123', got '%s'", rawPage.JobID)
	}

	if rawPage.ScopeHost != "127.0.0.1" {
		t.Errorf("expected scope_host to be propagated, got %q", rawPage.ScopeHost)
	}

	if len(rawPage.HTML) == 0 {
		t.Errorf("expected non-empty HTML body")
	}

	if result.Truncated {
		t.Errorf("small body should not be truncated")
	}

	if rawPage.DurationMs < 0 {
		t.Errorf("expected positive duration, got %d", rawPage.DurationMs)
	}
}

func TestFetcher_BlocksPrivateAddressesByDefault(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request should never reach a loopback server")
	}))
	defer ts.Close()

	cfg := testConfig()
	cfg.AllowPrivateNetworks = false
	f := New(cfg)

	_, err := f.Fetch(context.Background(), &models.CrawlTarget{URL: ts.URL})
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected ErrBlockedAddress, got %v", err)
	}
}

func TestGuardDial_BlocksCloudMetadataAddress(t *testing.T) {
	// The guard runs on every connection the client dials, including redirect
	// hops, so a public page redirecting here is blocked the same way.
	if err := guardDial("tcp4", "169.254.169.254:80", nil); !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("expected metadata address to be blocked, got %v", err)
	}
	if err := guardDial("tcp4", "93.184.216.34:443", nil); err != nil {
		t.Fatalf("expected public address to be allowed, got %v", err)
	}
}

func TestIsPublicAddr(t *testing.T) {
	cases := map[string]bool{
		"93.184.216.34":    true,
		"2606:4700::1111":  true,
		"127.0.0.1":        false,
		"10.1.2.3":         false,
		"172.16.0.1":       false,
		"192.168.1.1":      false,
		"169.254.169.254":  false,
		"100.64.0.1":       false,
		"0.0.0.0":          false,
		"::1":              false,
		"fe80::1":          false,
		"fd00::1":          false,
		"::ffff:127.0.0.1": false,
		"224.0.0.1":        false,
	}
	for addr, want := range cases {
		if got := isPublicAddr(netip.MustParseAddr(addr)); got != want {
			t.Errorf("isPublicAddr(%s) = %v, want %v", addr, got, want)
		}
	}
}

func TestFetcher_TruncatesOversizedBodies(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(strings.Repeat("a", 2048)))
	}))
	defer ts.Close()

	cfg := testConfig()
	cfg.MaxBodyBytes = 1024
	result, err := New(cfg).Fetch(context.Background(), &models.CrawlTarget{URL: ts.URL})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Truncated || len(result.Page.HTML) != 1024 {
		t.Fatalf("expected truncation to 1024 bytes, got truncated=%v len=%d", result.Truncated, len(result.Page.HTML))
	}
}

func TestFetcher_ReportsRetryAfter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	result, err := New(testConfig()).Fetch(context.Background(), &models.CrawlTarget{URL: ts.URL})
	if err != nil {
		t.Fatalf("non-2xx must not be an error: %v", err)
	}
	if result.Page.StatusCode != http.StatusTooManyRequests || result.RetryAfter != 2*time.Minute {
		t.Fatalf("got status=%d retryAfter=%v", result.Page.StatusCode, result.RetryAfter)
	}
}

func TestIsHTML(t *testing.T) {
	cases := []struct {
		contentType, body string
		want              bool
	}{
		{"text/html; charset=utf-8", "", true},
		{"application/xhtml+xml", "", true},
		{"application/pdf", "%PDF-1.7", false},
		{"image/png", "", false},
		{"application/json", "{}", false},
		{"", "<!DOCTYPE html><html><body>hi</body></html>", true},
		{"", "%PDF-1.7", false},
	}
	for _, c := range cases {
		if got := IsHTML(c.contentType, c.body); got != c.want {
			t.Errorf("IsHTML(%q, %q) = %v, want %v", c.contentType, c.body, got, c.want)
		}
	}
}
