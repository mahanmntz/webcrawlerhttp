// Package robots enforces robots.txt rules and Crawl-delay.
//
// Policies are cached in Redis under robots:<host> (shared by every crawler
// instance) and in process memory for a few minutes to avoid a Redis round
// trip per URL.
package robots

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
)

const (
	keyPrefix      = "robots:"
	maxRobotsBytes = 500 * 1024 // Google ignores anything past 500 KiB.
	okTTL          = 24 * time.Hour
	serverErrorTTL = time.Hour
	memoryTTL      = 5 * time.Minute
	maxMemoryHosts = 10000
)

// Decision is the outcome of a robots.txt check.
type Decision struct {
	Allowed    bool
	CrawlDelay time.Duration
}

type cachedPolicy struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}

type memoryEntry struct {
	data    *robotstxt.RobotsData
	expires time.Time
}

type inflight struct {
	done chan struct{}
	data *robotstxt.RobotsData
	err  error
}

// Checker fetches, caches and evaluates robots.txt policies.
type Checker struct {
	rdb       *redis.Client
	client    *http.Client
	userAgent string // sent as the User-Agent header
	agentName string // matched against User-agent groups

	mu       sync.Mutex
	memory   map[string]memoryEntry
	inflight map[string]*inflight
}

// New creates a Checker. client should be the SSRF-guarded fetcher client.
func New(rdb *redis.Client, client *http.Client, userAgent, agentName string) *Checker {
	return &Checker{
		rdb:       rdb,
		client:    client,
		userAgent: userAgent,
		agentName: agentName,
		memory:    make(map[string]memoryEntry),
		inflight:  make(map[string]*inflight),
	}
}

// Check reports whether u may be fetched and the host's Crawl-delay.
// An error means robots.txt could not be retrieved (e.g. network failure);
// callers should treat that as a retryable failure.
func (c *Checker) Check(ctx context.Context, u *url.URL) (Decision, error) {
	data, err := c.policy(ctx, u)
	if err != nil {
		return Decision{}, err
	}

	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	if u.RawQuery != "" {
		path += "?" + u.RawQuery
	}
	// TestAgent, not FindGroup().Test: only TestAgent honours the blanket
	// allow/disallow that 4xx/5xx robots.txt responses produce.
	return Decision{
		Allowed:    data.TestAgent(path, c.agentName),
		CrawlDelay: data.FindGroup(c.agentName).CrawlDelay,
	}, nil
}

func (c *Checker) policy(ctx context.Context, u *url.URL) (*robotstxt.RobotsData, error) {
	host := u.Host

	c.mu.Lock()
	if entry, ok := c.memory[host]; ok && time.Now().Before(entry.expires) {
		c.mu.Unlock()
		return entry.data, nil
	}
	// Collapse concurrent lookups for the same host into one fetch.
	if call, ok := c.inflight[host]; ok {
		c.mu.Unlock()
		select {
		case <-call.done:
			return call.data, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &inflight{done: make(chan struct{})}
	c.inflight[host] = call
	c.mu.Unlock()

	call.data, call.err = c.load(ctx, u)

	c.mu.Lock()
	delete(c.inflight, host)
	if call.err == nil {
		if len(c.memory) >= maxMemoryHosts {
			c.memory = make(map[string]memoryEntry)
		}
		c.memory[host] = memoryEntry{data: call.data, expires: time.Now().Add(memoryTTL)}
	}
	c.mu.Unlock()
	close(call.done)

	return call.data, call.err
}

func (c *Checker) load(ctx context.Context, u *url.URL) (*robotstxt.RobotsData, error) {
	key := keyPrefix + u.Host

	if raw, err := c.rdb.Get(ctx, key).Result(); err == nil {
		var cached cachedPolicy
		if json.Unmarshal([]byte(raw), &cached) == nil {
			return parse(cached.Status, cached.Body), nil
		}
	} else if !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("robots cache lookup failed: %w", err)
	}

	status, body, err := c.fetch(ctx, u)
	if err != nil {
		return nil, err
	}

	ttl := okTTL
	if status >= 500 {
		ttl = serverErrorTTL
	}
	if payload, err := json.Marshal(cachedPolicy{Status: status, Body: body}); err == nil {
		_ = c.rdb.Set(ctx, key, payload, ttl).Err()
	}
	return parse(status, body), nil
}

func (c *Checker) fetch(ctx context.Context, u *url.URL) (int, string, error) {
	robotsURL := url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/robots.txt"}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL.String(), nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("robots.txt fetch failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRobotsBytes))
	if err != nil {
		return 0, "", fmt.Errorf("robots.txt read failed: %w", err)
	}
	return resp.StatusCode, string(body), nil
}

// parse applies Google's status semantics: 2xx is parsed, 4xx allows
// everything, 5xx disallows everything. Anything unparseable allows all.
func parse(status int, body string) *robotstxt.RobotsData {
	data, err := robotstxt.FromStatusAndString(status, body)
	if err != nil {
		data, _ = robotstxt.FromStatusAndString(http.StatusNotFound, "")
	}
	return data
}
