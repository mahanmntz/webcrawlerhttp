package fetcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"crawler-engine/internal/config"
	"crawler-engine/internal/models"
)

func TestFetcher_FetchSuccess(t *testing.T) {
	// Create mock HTTP server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html><head><title>Test Page</title></head><body><h1>Hello Crawler</h1></body></html>"))
	}))
	defer ts.Close()

	cfg := &config.Config{
		FetchTimeout: 2 * time.Second,
		UserAgent:    "TestCrawler/1.0",
	}

	f := New(cfg)

	target := &models.CrawlTarget{
		JobID:     "test-job-123",
		URL:       ts.URL,
		Depth:     1,
		MaxDepth:  3,
		Priority:  5,
		CreatedAt: time.Now(),
	}

	ctx := context.Background()
	rawPage, err := f.Fetch(ctx, target)
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}

	if rawPage.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", rawPage.StatusCode)
	}

	if rawPage.JobID != "test-job-123" {
		t.Errorf("expected job_id 'test-job-123', got '%s'", rawPage.JobID)
	}

	if len(rawPage.HTML) == 0 {
		t.Errorf("expected non-empty HTML body")
	}

	if rawPage.DurationMs < 0 {
		t.Errorf("expected positive duration, got %d", rawPage.DurationMs)
	}
}
