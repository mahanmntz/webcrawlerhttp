package fetcher

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"crawler-engine/internal/config"
	"crawler-engine/internal/models"
)

// Fetcher executes tuned HTTP requests with connection pooling and timeouts.
type Fetcher struct {
	client    *http.Client
	userAgent string
}

// New creates a production-tuned Fetcher instance.
func New(cfg *config.Config) *Fetcher {
	// Custom transport ensures TCP connection reuse and bounds socket lifecycle.
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false, // Enable transparent gzip decompression
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.FetchTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("stopped after 5 redirects")
			}
			return nil
		},
	}

	return &Fetcher{
		client:    client,
		userAgent: cfg.UserAgent,
	}
}

// Fetch downloads the target URL and returns a structured RawPage model.
func (f *Fetcher) Fetch(ctx context.Context, target *models.CrawlTarget) (*models.RawPage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	start := time.Now()
	resp, err := f.client.Do(req)
	duration := time.Since(start).Milliseconds()

	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Guard against memory exhaustion: cap response body at 5MB
	limitReader := io.LimitReader(resp.Body, 5*1024*1024)
	bodyBytes, err := io.ReadAll(limitReader)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")

	rawPage := &models.RawPage{
		JobID:       target.JobID,
		URL:         resp.Request.URL.String(), // Effective URL after any redirects
		StatusCode:  resp.StatusCode,
		ContentType: contentType,
		Depth:       target.Depth,
		MaxDepth:    target.MaxDepth,
		HTML:        string(bodyBytes),
		DurationMs:  duration,
		FetchedAt:   time.Now().UTC(),
	}

	return rawPage, nil
}
