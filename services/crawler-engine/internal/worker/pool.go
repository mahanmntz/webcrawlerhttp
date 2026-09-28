package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"crawler-engine/internal/fetcher"
	"crawler-engine/internal/frontier"
	"crawler-engine/internal/politeness"
)

// Pool orchestrates a fixed number of worker goroutines consuming from the Frontier.
type Pool struct {
	workerCount int
	frontier    *frontier.RedisFrontier
	fetcher     *fetcher.Fetcher
	limiter     *politeness.Limiter
	wg          sync.WaitGroup
}

// New creates a new worker Pool.
func New(
	workerCount int,
	frontier *frontier.RedisFrontier,
	fetcher *fetcher.Fetcher,
	limiter *politeness.Limiter,
) *Pool {
	return &Pool{
		workerCount: workerCount,
		frontier:    frontier,
		fetcher:     fetcher,
		limiter:     limiter,
	}
}

// Start launches worker goroutines and blocks until context cancellation.
func (p *Pool) Start(ctx context.Context) {
	log.Printf("[WorkerPool] Starting %d concurrent crawler workers...", p.workerCount)

	for i := 1; i <= p.workerCount; i++ {
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

func (p *Pool) workerLoop(ctx context.Context, workerID string) {
	defer p.wg.Done()
	log.Printf("[%s] Worker initialized and listening for jobs.", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Worker received shutdown signal. Exiting loop.", workerID)
			return
		default:
		}

		// 1. Reliable Dequeue from Redis
		target, rawJSON, err := p.frontier.DequeueReliable(ctx, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] Error dequeuing from frontier: %v", workerID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if target == nil {
			// Frontier empty, wait for new targets
			continue
		}

		// 2. Enforce Politeness Rate-Limiting per Host
		acquired, _, err := p.limiter.AcquireLease(ctx, target.URL, workerID)
		if err != nil {
			log.Printf("[%s] Politeness check error: %v", workerID, err)
		}

		if !acquired {
			// Host is rate-limited: requeue target back to pending and yield worker briefly
			if err := p.frontier.Requeue(ctx, rawJSON); err != nil {
				log.Printf("[%s] Failed to requeue target: %v", workerID, err)
			}
			time.Sleep(150 * time.Millisecond)
			continue
		}

		// 3. Execute HTTP Download with Connection Pooling
		log.Printf("[%s] 🌐 FETCH  %s (Depth: %d/%d)", workerID, target.URL, target.Depth, target.MaxDepth)
		rawPage, fetchErr := p.fetcher.Fetch(ctx, target)

		if fetchErr != nil {
			log.Printf("[%s] ⚠️  FAIL   %s: %v", workerID, target.URL, fetchErr)
			// Acknowledge to unblock processing queue
			_ = p.frontier.Acknowledge(ctx, rawJSON)
			continue
		}

		// 4. Deliver Raw HTML Payload to Parser Queue
		if err := p.frontier.PushRawPage(ctx, rawPage); err != nil {
			log.Printf("[%s] ❌ QUEUE  Failed pushing RawPage to queue: %v", workerID, err)
		} else {
			log.Printf("[%s] ✅ OK     %s (%dms | HTTP %d | %d bytes)",
				workerID, rawPage.URL, rawPage.DurationMs, rawPage.StatusCode, len(rawPage.HTML))
		}

		// 5. Acknowledge task completion
		if err := p.frontier.Acknowledge(ctx, rawJSON); err != nil {
			log.Printf("[%s] Failed to acknowledge task: %v", workerID, err)
		}
	}
}
