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
	"crawler-engine/internal/robots"
	"crawler-engine/internal/scope"
)

const (
	bookkeepingTimeout = 5 * time.Second
	routeInterval      = 100 * time.Millisecond
	promoteInterval    = 250 * time.Millisecond
	reapInterval       = 5 * time.Second
	idleWaitMax        = 250 * time.Millisecond
	idleWaitMin        = 10 * time.Millisecond
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
	WorkerCount int
	MaxAttempts int
	// PolitenessDelay is the minimum gap between two requests to one host.
	PolitenessDelay time.Duration
	MaxCrawlDelay   time.Duration
}

// Pool orchestrates a fixed number of worker goroutines consuming from the Frontier.
type Pool struct {
	opts     Options
	frontier *frontier.RedisFrontier
	fetcher  *fetcher.Fetcher
	robots   *robots.Checker // nil when robots.txt is not enforced
	wg       sync.WaitGroup

	// bookkeepingTimeout bounds each Redis write that settles a target.
	bookkeepingTimeout time.Duration
}

// New creates a new worker Pool. robotsChecker may be nil.
func New(opts Options, frontier *frontier.RedisFrontier, fetcher *fetcher.Fetcher, robotsChecker *robots.Checker) *Pool {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 1
	}
	return &Pool{
		opts:               opts,
		frontier:           frontier,
		fetcher:            fetcher,
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

// maintenanceLoop routes ingested targets to host queues, promotes due
// retries and reaps expired leases. Every step is an atomic script, so any
// number of crawler instances can run it concurrently.
func (p *Pool) maintenanceLoop(ctx context.Context) {
	defer p.wg.Done()

	route := time.NewTicker(routeInterval)
	defer route.Stop()
	promote := time.NewTicker(promoteInterval)
	defer promote.Stop()
	reap := time.NewTicker(reapInterval)
	defer reap.Stop()

	logErr := func(err error) {
		if err != nil && ctx.Err() == nil {
			log.Printf("[Maintenance] %v", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-route.C:
			// Drain the ingest list in batches.
			for {
				n, err := p.frontier.Route(ctx)
				logErr(err)
				if err != nil || n == 0 {
					break
				}
			}
		case <-promote.C:
			_, err := p.frontier.PromoteDelayed(ctx)
			logErr(err)
		case <-reap.C:
			requeued, dead, err := p.frontier.ReapExpired(ctx)
			logErr(err)
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
		claim, wait, err := p.frontier.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("[%s] Error claiming from frontier: %v", workerID, err)
			sleep(ctx, 500*time.Millisecond)
			continue
		}
		if claim == nil {
			// Nothing ready: every host is cooling down or the frontier is empty.
			sleep(ctx, max(idleWaitMin, min(wait, idleWaitMax)))
			continue
		}
		p.Handle(ctx, workerID, claim)
	}
	log.Printf("[%s] Worker received shutdown signal. Exiting loop.", workerID)
}

// Handle processes one claimed target and always settles it. Exported for tests.
func (p *Pool) Handle(ctx context.Context, workerID string, claim *frontier.Claim) {
	// Settling helpers derive their own short-lived, uncancelable context
	// from bk, so they work during shutdown and after slow network calls.
	bk := context.WithoutCancel(ctx)
	target := claim.Target

	u, err := url.Parse(target.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		log.Printf("[%s] ☠️  DEAD   invalid URL %q", workerID, target.URL)
		p.deadLetter(bk, claim, "invalid URL", 0)
		return
	}

	// Gap before this host may be contacted again once we are done.
	hostDelay := p.opts.PolitenessDelay
	if p.robots != nil {
		decision, err := p.robots.Check(ctx, u)
		if err != nil {
			if ctx.Err() != nil {
				p.release(bk, claim)
				return
			}
			p.retry(bk, workerID, claim, err.Error(), 0, hostDelay)
			return
		}
		if !decision.Allowed {
			log.Printf("[%s] 🤖 SKIP   %s (disallowed by robots.txt)", workerID, target.URL)
			// No request was made, so the host is free again right away.
			p.finish(bk, claim, counterSkipped, 0)
			return
		}
		hostDelay = max(hostDelay, min(decision.CrawlDelay, p.opts.MaxCrawlDelay))
	}

	log.Printf("[%s] 🌐 FETCH  %s (Depth: %d/%d)", workerID, target.URL, target.Depth, target.MaxDepth)
	result, err := p.fetcher.Fetch(ctx, target)
	if err != nil {
		switch {
		case ctx.Err() != nil:
			p.release(bk, claim)
		case errors.Is(err, fetcher.ErrBlockedAddress):
			log.Printf("[%s] ☠️  DEAD   %s: %v", workerID, target.URL, err)
			p.deadLetter(bk, claim, err.Error(), 0)
		default:
			p.retry(bk, workerID, claim, err.Error(), 0, hostDelay)
		}
		return
	}

	page := result.Page
	switch {
	case page.StatusCode == http.StatusTooManyRequests || page.StatusCode >= 500:
		// The host is struggling: keep it closed at least until Retry-After.
		p.retry(bk, workerID, claim, fmt.Sprintf("HTTP %d", page.StatusCode), result.RetryAfter,
			max(hostDelay, min(result.RetryAfter, retryAfterMax)))

	case page.StatusCode < 200 || page.StatusCode >= 300:
		log.Printf("[%s] ⚠️  FAIL   %s: HTTP %d", workerID, target.URL, page.StatusCode)
		p.finish(bk, claim, counterFailed, hostDelay)

	case target.StayInDomain && !redirectInScope(page.URL, target.ScopeHost, u.Hostname()):
		log.Printf("[%s] 🧭 SKIP   %s redirected out of scope to %s", workerID, target.URL, page.URL)
		p.finish(bk, claim, counterSkipped, hostDelay)

	case !fetcher.IsHTML(page.ContentType, page.HTML):
		log.Printf("[%s] 📦 SKIP   %s (not HTML: %q)", workerID, target.URL, page.ContentType)
		p.finish(bk, claim, counterSkipped, hostDelay)

	default:
		if result.Truncated {
			log.Printf("[%s] ✂️  TRUNC  %s body exceeded size cap and was truncated", workerID, target.URL)
		}
		ctx, cancel := p.bookkeeping(bk)
		defer cancel()
		if _, err := p.frontier.PushRawPage(ctx, claim, page, hostDelay); err != nil {
			// Not committed: either the lease was lost (another worker owns the
			// target now) or Redis failed and the reaper will redeliver it.
			log.Printf("[%s] ❌ QUEUE  %s not handed to parser: %v", workerID, target.URL, err)
			return
		}
		log.Printf("[%s] ✅ OK     %s (%dms | HTTP %d | %d bytes)",
			workerID, page.URL, page.DurationMs, page.StatusCode, len(page.HTML))
		p.frontier.IncrJobCounter(ctx, target.JobID, counterFetched)
	}
}

// retry schedules another attempt with exponential backoff, or dead-letters
// the target once it has used up its attempts.
func (p *Pool) retry(ctx context.Context, workerID string, claim *frontier.Claim, reason string, retryAfter, hostDelay time.Duration) {
	target := claim.Target
	next := *target
	next.Attempts++
	if next.Attempts >= p.opts.MaxAttempts {
		log.Printf("[%s] ☠️  DEAD   %s after %d attempts: %s", workerID, target.URL, next.Attempts, reason)
		p.deadLetter(ctx, claim, fmt.Sprintf("%s (after %d attempts)", reason, next.Attempts), hostDelay)
		return
	}

	backoff := min(retryBaseDelay<<(next.Attempts-1), retryMaxDelay)
	backoff = max(backoff, min(retryAfter, retryAfterMax))
	nextJSON, err := json.Marshal(next)
	if err != nil {
		p.deadLetter(ctx, claim, "failed to marshal retry: "+err.Error(), hostDelay)
		return
	}

	log.Printf("[%s] 🔁 RETRY  %s in %v (attempt %d/%d): %s",
		workerID, target.URL, backoff.Round(time.Second), next.Attempts+1, p.opts.MaxAttempts, reason)

	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if p.logSettle(p.frontier.Delay(ctx, claim, string(nextJSON), backoff+jitter(backoff/5), hostDelay)) {
		p.frontier.IncrJobCounter(ctx, target.JobID, counterRetries)
	}
}

// bookkeeping returns a context for one settling write: detached from
// shutdown cancellation, with its own deadline.
func (p *Pool) bookkeeping(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), p.bookkeepingTimeout)
}

// logSettle reports whether a settle committed, logging why not otherwise.
func (p *Pool) logSettle(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, frontier.ErrStale):
		log.Printf("[WorkerPool] %v", err)
	default:
		log.Printf("[WorkerPool] %v (the reaper will redeliver the target)", err)
	}
	return false
}

func (p *Pool) finish(ctx context.Context, claim *frontier.Claim, counter string, hostDelay time.Duration) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if p.logSettle(p.frontier.Finish(ctx, claim, hostDelay)) {
		p.frontier.IncrJobCounter(ctx, claim.Target.JobID, counter)
	}
}

func (p *Pool) release(ctx context.Context, claim *frontier.Claim) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	p.logSettle(p.frontier.Release(ctx, claim))
}

func (p *Pool) deadLetter(ctx context.Context, claim *frontier.Claim, reason string, hostDelay time.Duration) {
	ctx, cancel := p.bookkeeping(ctx)
	defer cancel()
	if p.logSettle(p.frontier.DeadLetter(ctx, claim, reason, hostDelay)) {
		p.frontier.IncrJobCounter(ctx, claim.Target.JobID, counterFailed)
	}
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
