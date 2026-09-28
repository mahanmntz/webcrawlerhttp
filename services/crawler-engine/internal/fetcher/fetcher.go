package fetcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"syscall"
	"time"

	"crawler-engine/internal/config"
	"crawler-engine/internal/models"
)

// ErrBlockedAddress is returned when a URL resolves to a loopback, private,
// link-local or otherwise non-public address (SSRF guard).
var ErrBlockedAddress = errors.New("destination address is not publicly routable")

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

func isPublicAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	return addr.IsGlobalUnicast() &&
		!addr.IsPrivate() &&
		!addr.IsLoopback() &&
		!addr.IsLinkLocalUnicast() &&
		!cgnat.Contains(addr)
}

// guardDial rejects connections to non-public IPs. It runs after DNS
// resolution, so it also covers redirects and DNS rebinding.
func guardDial(network, address string, _ syscall.RawConn) error {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlockedAddress, address)
	}
	if !isPublicAddr(addrPort.Addr()) {
		return fmt.Errorf("%w: %s", ErrBlockedAddress, addrPort.Addr())
	}
	return nil
}

// Fetcher executes tuned HTTP requests with connection pooling and timeouts.
type Fetcher struct {
	client       *http.Client
	userAgent    string
	maxBodyBytes int64
}

// New creates a production-tuned Fetcher instance.
func New(cfg *config.Config) *Fetcher {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if !cfg.AllowPrivateNetworks {
		dialer.Control = guardDial
	}

	// Custom transport ensures TCP connection reuse and bounds socket lifecycle.
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
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

	maxBody := cfg.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = 5 * 1024 * 1024
	}

	return &Fetcher{
		client:       client,
		userAgent:    cfg.UserAgent,
		maxBodyBytes: maxBody,
	}
}

// Client exposes the guarded HTTP client for auxiliary fetches (robots.txt).
func (f *Fetcher) Client() *http.Client { return f.client }

// UserAgent returns the User-Agent header sent with every request.
func (f *Fetcher) UserAgent() string { return f.userAgent }

// Result is a completed HTTP exchange. Non-2xx responses are results, not
// errors; the caller decides whether to retry.
type Result struct {
	Page       *models.RawPage
	Truncated  bool
	RetryAfter time.Duration
}

// Fetch downloads the target URL. An error means no HTTP response was received.
func (f *Fetcher) Fetch(ctx context.Context, target *models.CrawlTarget) (*Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", f.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	start := time.Now()
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Guard against memory exhaustion; read one extra byte to detect truncation.
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	truncated := int64(len(bodyBytes)) > f.maxBodyBytes
	if truncated {
		bodyBytes = bodyBytes[:f.maxBodyBytes]
	}
	duration := time.Since(start).Milliseconds()

	return &Result{
		Page: &models.RawPage{
			JobID:        target.JobID,
			URL:          resp.Request.URL.String(), // Effective URL after any redirects
			StatusCode:   resp.StatusCode,
			ContentType:  resp.Header.Get("Content-Type"),
			Depth:        target.Depth,
			MaxDepth:     target.MaxDepth,
			StayInDomain: target.StayInDomain,
			ScopeHost:    target.ScopeHost,
			HTML:         string(bodyBytes),
			DurationMs:   duration,
			FetchedAt:    time.Now().UTC(),
		},
		Truncated:  truncated,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
	}, nil
}

// IsHTML reports whether a response looks like an HTML document, using the
// Content-Type header and falling back to content sniffing when it is absent.
func IsHTML(contentType, body string) bool {
	if contentType == "" {
		contentType = http.DetectContentType([]byte(body))
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return mediaType == "text/html" || mediaType == "application/xhtml+xml"
}

func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return time.Until(at)
	}
	return 0
}
