package frontier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
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
	FrontierLeases          = "frontier:leases"
	FrontierDelayed         = "frontier:delayed"
	FrontierDead            = "frontier:dead"
	FrontierRedeliveries    = "frontier:redeliveries"
	QueueRawPages           = "queue:raw_pages"
	promoteBatchSize        = 500
	deadLetterReasonCorrupt = "corrupted CrawlTarget JSON"
)

// reapLua re-queues items whose worker died mid-flight.
//
// Every item in the processing list has a lease in a sorted set, keyed by the
// SHA-1 of the item and scored by its deadline (ms). Items without a lease
// (the worker died between dequeue and registering it) get one now. Items past
// their deadline go back to the source queue, or to the dead-letter list once
// they have been redelivered more than max_redeliveries times.
//
// parser-scraper/app/reliable_queue.py runs the same script. Keep them in sync.
//
// KEYS: processing, leases, source, dead, redeliveries
// ARGV: now_ms, visibility_ms, max_redeliveries
var reapLua = redis.NewScript(`
local now = tonumber(ARGV[1])
local visibility = tonumber(ARGV[2])
local max_redeliveries = tonumber(ARGV[3])
local requeued, dead = 0, 0
local present = {}

for _, item in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  local id = redis.sha1hex(item)
  present[id] = true
  local deadline = redis.call('ZSCORE', KEYS[2], id)
  if not deadline then
    redis.call('ZADD', KEYS[2], now + visibility, id)
  elseif tonumber(deadline) <= now then
    redis.call('LREM', KEYS[1], 1, item)
    redis.call('ZREM', KEYS[2], id)
    if redis.call('HINCRBY', KEYS[5], id, 1) > max_redeliveries then
      redis.call('HDEL', KEYS[5], id)
      redis.call('LPUSH', KEYS[4], cjson.encode({
        payload = item, reason = 'exceeded max redeliveries', failed_at_ms = now
      }))
      dead = dead + 1
    else
      redis.call('RPUSH', KEYS[3], item)
      requeued = requeued + 1
    end
  end
end

for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)) do
  if not present[id] then
    redis.call('ZREM', KEYS[2], id)
  end
end

return {requeued, dead}
`)

// promoteLua moves delayed targets whose time has come to the head of the
// frontier (the end BLMOVE pops from).
//
// KEYS: delayed, frontier    ARGV: now_ms, limit
var promoteLua = redis.NewScript(`
local ready = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, item in ipairs(ready) do
  redis.call('ZREM', KEYS[1], item)
  redis.call('RPUSH', KEYS[2], item)
end
return #ready
`)

// DeadLetter is the envelope stored in frontier:dead.
type DeadLetter struct {
	Payload    string `json:"payload"`
	Reason     string `json:"reason"`
	FailedAtMs int64  `json:"failed_at_ms"`
}

// RedisFrontier encapsulates queue operations interacting with Redis.
type RedisFrontier struct {
	rdb               *redis.Client
	visibilityTimeout time.Duration
	maxRedeliveries   int
}

// New creates a new RedisFrontier client.
func New(rdb *redis.Client, visibilityTimeout time.Duration, maxRedeliveries int) *RedisFrontier {
	return &RedisFrontier{rdb: rdb, visibilityTimeout: visibilityTimeout, maxRedeliveries: maxRedeliveries}
}

func itemID(rawJSON string) string {
	sum := sha1.Sum([]byte(rawJSON))
	return hex.EncodeToString(sum[:])
}

// DequeueReliable atomically moves a target from frontier:queue to
// frontier:processing and registers a lease for it. Blocks up to timeout.
// Returns (nil, "", nil) when the queue is empty.
func (f *RedisFrontier) DequeueReliable(ctx context.Context, timeout time.Duration) (*models.CrawlTarget, string, error) {
	val, err := f.rdb.BLMove(ctx, QueueFrontierPending, QueueFrontierProcessing, "RIGHT", "LEFT", timeout).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("failed to pop from frontier: %w", err)
	}

	deadline := time.Now().Add(f.visibilityTimeout).UnixMilli()
	if err := f.rdb.ZAdd(ctx, FrontierLeases, redis.Z{Score: float64(deadline), Member: itemID(val)}).Err(); err != nil {
		// The reaper registers a lease for orphans, so this is recoverable.
		return nil, "", fmt.Errorf("failed to register lease: %w", err)
	}

	var target models.CrawlTarget
	if err := json.Unmarshal([]byte(val), &target); err != nil {
		if dlErr := f.DeadLetter(ctx, val, deadLetterReasonCorrupt+": "+err.Error()); dlErr != nil {
			return nil, "", dlErr
		}
		return nil, "", fmt.Errorf("%s: %w", deadLetterReasonCorrupt, err)
	}

	return &target, val, nil
}

// settle removes rawJSON from processing and drops its lease, plus whatever
// extra commands the caller queues, in one MULTI/EXEC.
func (f *RedisFrontier) settle(ctx context.Context, rawJSON string, then func(pipe redis.Pipeliner)) error {
	id := itemID(rawJSON)
	_, err := f.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LRem(ctx, QueueFrontierProcessing, 1, rawJSON)
		pipe.ZRem(ctx, FrontierLeases, id)
		pipe.HDel(ctx, FrontierRedeliveries, id)
		if then != nil {
			then(pipe)
		}
		return nil
	})
	return err
}

// Acknowledge removes a finished target from frontier:processing.
func (f *RedisFrontier) Acknowledge(ctx context.Context, rawJSON string) error {
	if err := f.settle(ctx, rawJSON, nil); err != nil {
		return fmt.Errorf("failed to acknowledge target: %w", err)
	}
	return nil
}

// Delay moves a target out of processing and schedules nextJSON (the same or
// an updated target) to be re-queued after delay.
func (f *RedisFrontier) Delay(ctx context.Context, rawJSON, nextJSON string, delay time.Duration) error {
	readyAt := time.Now().Add(delay).UnixMilli()
	err := f.settle(ctx, rawJSON, func(pipe redis.Pipeliner) {
		pipe.ZAdd(ctx, FrontierDelayed, redis.Z{Score: float64(readyAt), Member: nextJSON})
	})
	if err != nil {
		return fmt.Errorf("failed to delay target: %w", err)
	}
	return nil
}

// Release puts a target straight back at the head of the frontier, e.g. when
// the worker is shutting down mid-fetch.
func (f *RedisFrontier) Release(ctx context.Context, rawJSON string) error {
	err := f.settle(ctx, rawJSON, func(pipe redis.Pipeliner) {
		pipe.RPush(ctx, QueueFrontierPending, rawJSON)
	})
	if err != nil {
		return fmt.Errorf("failed to release target: %w", err)
	}
	return nil
}

// DeadLetter moves a target that cannot be processed to frontier:dead.
func (f *RedisFrontier) DeadLetter(ctx context.Context, rawJSON, reason string) error {
	envelope, err := json.Marshal(DeadLetter{Payload: rawJSON, Reason: reason, FailedAtMs: time.Now().UnixMilli()})
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter: %w", err)
	}
	err = f.settle(ctx, rawJSON, func(pipe redis.Pipeliner) {
		pipe.LPush(ctx, FrontierDead, string(envelope))
	})
	if err != nil {
		return fmt.Errorf("failed to dead-letter target: %w", err)
	}
	return nil
}

// PushRawPage delivers the downloaded raw HTML payload to the parser queue.
func (f *RedisFrontier) PushRawPage(ctx context.Context, page *models.RawPage) error {
	payload, err := json.Marshal(page)
	if err != nil {
		return fmt.Errorf("failed to marshal RawPage: %w", err)
	}

	if err := f.rdb.LPush(ctx, QueueRawPages, string(payload)).Err(); err != nil {
		return fmt.Errorf("failed to push RawPage to queue: %w", err)
	}
	return nil
}

// IncrJobCounter bumps a per-job progress counter (job:<id> hash).
func (f *RedisFrontier) IncrJobCounter(ctx context.Context, jobID, field string) {
	if jobID == "" {
		return
	}
	_ = f.rdb.HIncrBy(ctx, "job:"+jobID, field, 1).Err()
}

// PromoteDelayed moves delayed targets that are due back onto the frontier.
func (f *RedisFrontier) PromoteDelayed(ctx context.Context) (int, error) {
	n, err := promoteLua.Run(ctx, f.rdb, []string{FrontierDelayed, QueueFrontierPending},
		time.Now().UnixMilli(), promoteBatchSize).Int()
	if err != nil {
		return 0, fmt.Errorf("failed to promote delayed targets: %w", err)
	}
	return n, nil
}

// ReapExpired re-queues (or dead-letters) targets whose lease has expired.
func (f *RedisFrontier) ReapExpired(ctx context.Context) (requeued, dead int, err error) {
	res, err := reapLua.Run(ctx, f.rdb,
		[]string{QueueFrontierProcessing, FrontierLeases, QueueFrontierPending, FrontierDead, FrontierRedeliveries},
		time.Now().UnixMilli(), f.visibilityTimeout.Milliseconds(), f.maxRedeliveries,
	).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to reap expired leases: %w", err)
	}
	return int(res[0]), int(res[1]), nil
}
