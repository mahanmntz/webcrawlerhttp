package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"

	"crawler-engine/internal/fetcher"
	"crawler-engine/internal/frontier"
	"crawler-engine/internal/models"
	"crawler-engine/internal/politeness"
	"crawler-engine/internal/robots"
	"crawler-engine/internal/scope"
)

const (
	dequeueTimeout     = 2 * time.Second
	bookkeepingTimeout = 5 * time.Second
	promoteInterval    = 250 * time.Millisecond
	reapInterval       = 5 * time.Second
	retryBaseDelay     = 5 * time.Second
	retryMaxDelay      = 5 * time.Minute
	retryAfterMax      = time.Hour
)

// Job counters written to the job:<id> hash.
const (
	counterFetched = "pages_fetched"
	counterFailed  = "pages_failed"
	counterSkipped = "pages_skipped"
	counterRetries = "fetch_retries"
)

// Options configures a Pool.
type Options struct {
	WorkerCount   int
	MaxAttempts   int
	MaxCrawlDelay time.Duration
}

// Pool orchestrates a fixed number of worker goroutines consuming from the Frontier.
type Pool struct {
	opts     Options
	frontier *frontier.RedisFrontier
	fetcher  *fetcher.Fetcher
	limiter  *politeness.Limiter
	robots   *robots.Checker // nil when robots.txt is not enforced
	wg       sync.WaitGroup

	// bookkeepingTimeout bounds each Redis write that settles a target.
	bookkeepingTimeout time.Duration
}

// New creates a new worker Pool. robotsChecker may be nil.
func New(
	opts Options,
	frontier *frontier.RedisFrontier,
	fetcher *fetcher.Fetcher,
	limiter *politeness.Limiter,
	robotsChecker *robots.Checker,
) *Pool {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 1
	}
	return &Pool{
		opts:               opts,
		frontier:           frontier,
		fetcher:            fetcher,
		limiter:            limiter,
		robots:             robotsChecker,
		bookkeepingTimeout: bookkeepingTimeout,
	}
}

// Start launches worker goroutines plus the maintenance loop.
func (p *Pool) Start(ctx context.Context) {
	log.Printf("[WorkerPool] Starting %d concurrent crawler workers...", p.opts.WorkerCount)

	p.wg.Add(1)
	go p.maintenanceLoop(ctx)

	for i := 1; i <= p.opts.WorkerCount; i++ {
		workerID := fmt.Sprintf("worker-go-%02d", i)
		p.wg.Add(1)
		go p.workerLoop(ctx, workerID)
	}
}

// Stop waits for all active worker goroutines to finish gracefully.
func (p *Pool) Stop() {
	log.Println("[WorkerPool] Waiting for in-flight workers to finish...")
	p.wg.Wait()
	log.Println("[WorkerPool] All workers stopped cleanly.")
}

// maintenanceLoop promotes due delayed targets and reaps expired leases.
// Both operations are atomic scripts, so any number of crawler instances can
// run them concurrently.
func (p *Pool) maintenanceLoop(ctx context.Context) {
	defer p.wg.Done()

	promote := time.NewTicker(promoteInterval)
	defer promote.Stop()
	reap := time.NewTicker(reapInterval)
	defer reap.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-promote.C:
			if _, err := p.frontier.PromoteDelayed(ctx); err != nil && ctx.Err() == nil {
				log.Printf("[Maintenance] %v", err)
			}
		case <-reap.C:
			requeued, dead, err := p.frontier.ReapExpired(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[Maintenance] %v", err)
				}
				continue
			}
			if requeued > 0 || dead > 0 {
				log.Printf("[Maintenance] ♻️  Recovered %d stalled targets, dead-lettered %d", requeued, dead)
			}
		}
	}
}

func (p *Pool) workerLoop(ctx context.Context, workerID string) {
	defer p.wg.Done()
	log.Printf("[%s] Worker initialized and listening for jobs.", workerID)

	for ctx.Err() == nil {
		target, rawJSON, err := p.frontier.DequeueReliable(ctx, dequeueTimeout)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("[%s] Error dequeuing from frontier: %v", workerID, err)
			sleep(ctx, 500*time.Millisecond)
			continue
		}
		if target == nil {
			continue // Frontier empty
		}
		p.Handle(ctx, workerID, target, rawJSON)
	}
	log.Printf("[%s] Worker received shutdown signal. Exiting loop.", workerID)
}

// Handle processes one dequeued target and always settles it: acknowledged,
// delayed, released, or dead-lettered. Exported for tests.
func (p *Pool) Handle(ctx context.Context, workerID string, target *models.CrawlTarget, rawJSON string) {
	// Settling helpers derive their own short-lived, uncancelable context
	// from bk, so they work during shutdown and after slow network calls.
	bk := context.WithoutCancel(ctx)

	u, err := url.Parse(target.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		log.Printf("[%s] ☠️  DEAD   invalid URL %q", workerID, target.URL)
		p.deadLetter(bk, target, rawJSON, "invalid URL")
		return
	}

	delay := p.limiter.DefaultDelay()
	if p.robots != nil {
		decision, err := p.robots.Check(ctx, u)
		if err != nil {
			if ctx.Err() != nil {
				p.release(bk, rawJSON)
				return
			}
			p.retry(bk, workerID, target, rawJSON, err.Error(), 0)
			return
		}
		if !decision.Allowed {
			log.Printf("[%s] 🤖 SKIP   %s (disallowed by robots.txt)", workerID, target.URL)
			p.ack(bk, target, rawJSON, counterSkipped)
			return
		}
		delay = max(delay, min(decision.CrawlDelay, p.opts.MaxCrawlDelay))
	}

	wait, err := p.limiter.AcquireLease(ctx, u.Hostname(), workerID, delay)
	if err != nil {
		if ctx.Err() != nil {
			p.release(bk, rawJSON)
			return
		}
		log.Printf("[%s] Politeness check error: %v", workerID, err)
		p.delay(bk, rawJSON, rawJSON, time.Second)
		return
	}
	if wait > 0 {
		// Host is cooling down. Park the target instead of spinning on it; the
		// jitter spreads out several targets waiting on the same host.
		p.delay(bk, rawJSON, rawJSON, wait+jitter(delay))
		return
	}

	log.Printf("[%s] 🌐 FETCH  %s (Depth: %d/%d)", workerID, target.URL, target.Depth, target.MaxDepth)
	result, err := p.fetcher.Fetch(ctx, target)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			p.release(bk, rawJSON)
		case errors.Is(err, fetcher.ErrBlockedAddress):
			log.Printf("[%s] ☠️  DEAD   %s: %v", workerID, target.URL, err)
			p.deadLetter(bk, target, rawJSON, err.Error())
		default:
			p.retry(bk, workerID, target, rawJSON, err.Error(), 0)
		}
		return
	}

	page := result.Page
	switch {
	case page.StatusCode == http.StatusTooManyRequests || page.StatusCode >= 500:
		p.retry(bk, workerID, target, rawJSON, fmt.Sprintf("HTTP %d", page.StatusCode), result.RetryAfter)

	case page.StatusCode < 200 || page.StatusCode >= 300:
		log.Printf("[%s] ⚠️  FAIL   %s: HTTP %d", workerID, target.URL, page.StatusCode)
		p.ack(bk, target, rawJSON, counterFailed)

	case target.StayInDomain && !redirectInScope(page.URL, target.ScopeHost, u.Hostname()):
		log.Printf("[%s] 🧭 SKIP   %s redirected out of scope to %s", workerID, target.URL, page.URL)
		p.ack(bk, target, rawJSON, counterSkipped)

	case !fetcher.IsHTML(page.ContentType, page.HTML):
		log.Printf("[%s] 📦 SKIP   %s (not HTML: %q)", workerID, target.URL, page.ContentType)
		p.ack(bk, target, rawJSON, counterSkipped)

	default:
		if result.Truncated {
			log.Printf("[%s] ✂️  TRUNC  %s body exceeded size cap and was truncated", workerID, target.URL)
		}
		pushCtx, cancel := p.bookkeeping(bk)
		err := p.frontier.PushRawPage(pushCtx, page)
		cancel()
		if err != nil {
			// Leave the target in processing; the reaper will redeliver it.
			log.Printf("[%s] ❌ QUEUE  Failed pushing RawPage to queue: %v", workerID, err)
			return
		}
		log.Printf("[%s] ✅ OK     %s (%dms | HTTP %d | %d bytes)",
			workerID, page.URL, page.DurationMs, page.StatusCode, len(page.HTML))
		p.ack(bk, target, rawJSON, counterFetched)
	}
}

// retry schedules another attempt with exponential backoff, or dead-letters
// the target once it has used up its attempts.
func (p *Pool) retry(ctx context.Context, workerID string, target *models.CrawlTarget, rawJSON, reason string, retryAfter time.Duration) {
	next := *target
	next.Attempts++
	if next.Attempts >= p.opts.MaxAttempts {
		log.Printf("[%s] ☠️  DEAD   %s after %d attempts: %s", workerID, target.URL, next.Attempts, reason)
		p.deadLetter(ctx, target, rawJSON, fmt.Sprintf("%s (after %d attempts)", reason, next.Attempts))
		return
	}

	backoff := min(retryBaseDelay<<(next.Attempts-1), retryMaxDelay)
	backoff = max(backoff, min(retryAfter, retryAfterMax))
	nextJSON, err := json.Marshal(next)
	if err != nil {
		p.deadLetter(ctx, target, rawJSON, "failed to marshal retry: "+err.Error())
		return
	}

	log.Printf("[%s] 🔁 RETRY  %s in %v (attempt %d/%d): %s",
		workerID, target.URL, backoff.Round(time.Second), next.Attempts+1, p.opts.MaxAttempts, reason)
	p.delay(ctx, rawJSON, string(nextJSON), backoff+jitter(backoff/5))
	counterCtx, cancel := p.bookkeeping(ctx)
	defer cancel()
	p.frontier.IncrJobCounter(counterCtx, target.JobID, counterRetries)
}

// bookkeeping returns a context for one settling write: detached from
// shutdown cancellation, with its own deadline.
func (p *Pool) bookkeeping(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), p.bookkeepingTimeout)
}

func (p *Pool) ack(ctx context.Context, target *models.CrawlTarget, rawJSON, counter string) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if err := p.frontier.Acknowledge(ctx, rawJSON); err != nil {
		log.Printf("[WorkerPool] %v", err)
		return
	}
	p.frontier.IncrJobCounter(ctx, target.JobID, counter)
}

func (p *Pool) delay(ctx context.Context, rawJSON, nextJSON string, d time.Duration) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if err := p.frontier.Delay(ctx, rawJSON, nextJSON, d); err != nil {
		log.Printf("[WorkerPool] %v", err)
	}
}

func (p *Pool) release(ctx context.Context, rawJSON string) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if err := p.frontier.Release(ctx, rawJSON); err != nil {
		log.Printf("[WorkerPool] %v", err)
	}
}

func (p *Pool) deadLetter(ctx context.Context, target *models.CrawlTarget, rawJSON, reason string) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if err := p.frontier.DeadLetter(ctx, rawJSON, reason); err != nil {
		log.Printf("[WorkerPool] %v", err)
		return
	}
	p.frontier.IncrJobCounter(ctx, target.JobID, counterFailed)
}

// redirectInScope checks the post-redirect URL against the crawl boundary.
// Targets enqueued before scope_host existed fall back to their own host.
func redirectInScope(finalURL, scopeHost, requestedHost string) bool {
	if scopeHost == "" {
		scopeHost = requestedHost
	}
	u, err := url.Parse(finalURL)
	return err == nil && scope.InScope(u.Hostname(), scopeHost)
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
