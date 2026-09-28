package frontier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"crawler-engine/internal/models"

	"github.com/redis/go-redis/v9"
)

const (
	QueueFrontierPending    = "frontier:queue"
	QueueFrontierProcessing = "frontier:processing"
	QueueRawPages           = "queue:raw_pages"
)

// RedisFrontier encapsulates queue operations interacting with Redis.
type RedisFrontier struct {
	rdb *redis.Client
}

// New creates a new RedisFrontier client.
func New(rdb *redis.Client) *RedisFrontier {
	return &RedisFrontier{rdb: rdb}
}

// DequeueReliable atomically pops a job from frontier:queue and pushes to frontier:processing.
// Blocks up to timeout duration if queue is empty.
func (f *RedisFrontier) DequeueReliable(ctx context.Context, timeout time.Duration) (*models.CrawlTarget, string, error) {
	val, err := f.rdb.BRPopLPush(ctx, QueueFrontierPending, QueueFrontierProcessing, timeout).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, "", nil // Queue empty
		}
		return nil, "", fmt.Errorf("failed to pop from frontier: %w", err)
	}

	var target models.CrawlTarget
	if err := json.Unmarshal([]byte(val), &target); err != nil {
		// If corrupted, remove from processing queue to avoid permanent stall
		_ = f.rdb.LRem(ctx, QueueFrontierProcessing, 1, val).Err()
		return nil, "", fmt.Errorf("corrupted CrawlTarget JSON: %w", err)
	}

	return &target, val, nil
}

// Requeue moves a deferred/rate-limited target back to the frontier queue.
func (f *RedisFrontier) Requeue(ctx context.Context, rawJSON string) error {
	pipe := f.rdb.Pipeline()
	pipe.LPush(ctx, QueueFrontierPending, rawJSON)
	pipe.LRem(ctx, QueueFrontierProcessing, 1, rawJSON)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to requeue target: %w", err)
	}
	return nil
}

// Acknowledge removes a successfully crawled target from frontier:processing.
func (f *RedisFrontier) Acknowledge(ctx context.Context, rawJSON string) error {
	err := f.rdb.LRem(ctx, QueueFrontierProcessing, 1, rawJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to acknowledge target in processing queue: %w", err)
	}
	return nil
}

// PushRawPage delivers the downloaded raw HTML payload to the parser queue.
func (f *RedisFrontier) PushRawPage(ctx context.Context, page *models.RawPage) error {
	payload, err := json.Marshal(page)
	if err != nil {
		return fmt.Errorf("failed to marshal RawPage: %w", err)
	}

	err = f.rdb.LPush(ctx, QueueRawPages, string(payload)).Err()
	if err != nil {
		return fmt.Errorf("failed to push RawPage to queue: %w", err)
	}

	return nil
}
