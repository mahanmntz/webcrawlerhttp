// Package frontier implements the URL frontier as per-host back queues in Redis
// (System Design Interview, ch. 9):
//
//   - Producers LPUSH CrawlTargets onto frontier:queue (the ingest list).
//   - Route moves them into one sorted set per host (frontier:host:<host>),
//     ordered by priority then arrival, and registers the host in
//     frontier:hosts, a sorted set of host -> time it may next be contacted.
//   - Claim atomically picks a host whose time has come, pops its best target
//     into frontier:processing (with a lease) and locks the host until the
//     target is settled. Politeness is therefore structural: one request in
//     flight per host, and a delay after each one.
//   - Settle commits a worker's outcome in one script, fenced on the target
//     still being in processing: if the reaper already handed it to another
//     worker, the stale outcome is discarded. Effects are exactly-once even
//     though delivery is at-least-once.
//
// See shared/contracts/REDIS_SPEC.md for the full key layout.
package frontier

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"crawler-engine/internal/models"

	"github.com/redis/go-redis/v9"
)

const (
	QueueFrontierIngest     = "frontier:queue"
	QueueFrontierProcessing = "frontier:processing"
	FrontierHosts           = "frontier:hosts"
	FrontierHostPrefix      = "frontier:host:"
	FrontierScheduled       = "frontier:scheduled"
	FrontierLeases          = "frontier:leases"
	FrontierDelayed         = "frontier:delayed"
	FrontierDead            = "frontier:dead"
	FrontierRedeliveries    = "frontier:redeliveries"
	QueueRawPages           = "queue:raw_pages"
	RawPagePrefix           = "raw_page:"

	// RawPageTTL bounds how long a fetched page waits for the parser.
	RawPageTTL = 7 * 24 * time.Hour

	routeBatchSize   = 1000
	promoteBatchSize = 500
	claimScanLimit   = 50
)

// QueueFrontierPending is kept for callers that push seeds directly.
const QueueFrontierPending = QueueFrontierIngest

// routeLua moves ingested targets into their host's queue.
//
// KEYS: ingest, hosts, scheduled, dead    ARGV: now_ms, batch
var routeScript = redis.NewScript(jobsLua + `
local now = tonumber(ARGV[1])
local moved = 0
for _ = 1, tonumber(ARGV[2]) do
  local item = redis.call('RPOP', KEYS[1])
  if not item then
    break
  end
  local ok, t = pcall(cjson.decode, item)
  local host = nil
  if ok and type(t) == 'table' and type(t.url) == 'string' then
    host = string.match(t.url, '^[hH][tT][tT][pP][sS]?://([^/?#]+)')
  end
  if not host then
    redis.call('LPUSH', KEYS[4], cjson.encode({
      payload = item, reason = 'unroutable target', failed_at_ms = now
    }))
    finish_job(job_key_from_json(item))
  else
    host = string.lower(host)
    local priority = math.max(1, math.min(10, tonumber(t.priority) or 5))
    -- Higher priority first, then FIFO. Fits exactly in a double.
    local score = (10 - priority) * 10000000000000 + now
    if redis.call('ZADD', 'frontier:host:' .. host, 'NX', score, item) == 1 then
      redis.call('ZADD', KEYS[2], 'NX', now, host)
      redis.call('INCR', KEYS[3])
      moved = moved + 1
    else
      -- An identical target is already scheduled.
      finish_job(job_key_from_json(item))
    end
  end
end
return moved
`)

// claimLua takes the best target of the first host that may be contacted.
//
// KEYS: hosts, processing, leases, scheduled
// ARGV: now_ms, host_lock_ms, lease_ms, scan_limit
// Returns {host, item}; {} if no host is known; {”, ”, next_ready_ms} if
// every host is still cooling down.
var claimScript = redis.NewScript(jobsLua + `
local now = tonumber(ARGV[1])
local ready = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[4]))
for _, host in ipairs(ready) do
  local popped = redis.call('ZPOPMIN', 'frontier:host:' .. host)
  if #popped == 0 then
    -- Nothing left for this host, and its politeness delay has passed.
    redis.call('ZREM', KEYS[1], host)
  else
    local item = popped[1]
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), host)
    redis.call('LPUSH', KEYS[2], item)
    redis.call('ZADD', KEYS[3], now + tonumber(ARGV[3]), redis.sha1hex(item))
    redis.call('DECR', KEYS[4])
    local job = job_key_from_json(item)
    if job and redis.call('HGET', job, 'status') == 'enqueued' then
      redis.call('HSET', job, 'status', 'running')
    end
    return {host, item}
  end
end
local next_host = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #next_host == 0 then
  return {}
end
return {'', '', next_host[2]}
`)

// settleLua commits a claimed target's outcome, fenced on the target still
// being in processing. Returns 0 (and changes nothing) if it is not.
//
// KEYS: processing, leases, redeliveries, hosts, dest, job, payload_key
// ARGV: item, host, host_ready_ms, mode, a1, a2, a3
//
//	push:    SET payload_key a1 EX a2; LPUSH dest a3   (page for the parser)
//	finish:  finish the job                            (terminal, nothing to parse)
//	dead:    LPUSH dest a1; finish the job             (dead letter)
//	delay:   ZADD dest a1 a2                           (retry later)
//	release: RPUSH dest item                           (back to ingest, e.g. shutdown)
var settleScript = redis.NewScript(jobsLua + `
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
local id = redis.sha1hex(ARGV[1])
redis.call('ZREM', KEYS[2], id)
redis.call('HDEL', KEYS[3], id)
if ARGV[2] ~= '' then
  redis.call('ZADD', KEYS[4], 'XX', ARGV[3], ARGV[2])
end

local mode = ARGV[4]
if mode == 'push' then
  redis.call('SET', KEYS[7], ARGV[5], 'EX', ARGV[6])
  redis.call('LPUSH', KEYS[5], ARGV[7])
elseif mode == 'finish' then
  finish_job(KEYS[6])
elseif mode == 'dead' then
  redis.call('LPUSH', KEYS[5], ARGV[5])
  finish_job(KEYS[6])
elseif mode == 'delay' then
  redis.call('ZADD', KEYS[5], ARGV[5], ARGV[6])
elseif mode == 'release' then
  redis.call('RPUSH', KEYS[5], ARGV[1])
end
return 1
`)

// promoteLua moves delayed targets that are due back to ingest.
//
// KEYS: delayed, ingest    ARGV: now_ms, limit
var promoteScript = redis.NewScript(`
local ready = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, item in ipairs(ready) do
  redis.call('ZREM', KEYS[1], item)
  redis.call('RPUSH', KEYS[2], item)
end
return #ready
`)

var reapScript = redis.NewScript(jobsLua + reapLua)

// ErrStale is returned by settle operations when the target was no longer in
// processing (its lease expired and it was redelivered). The outcome was
// discarded; the worker holding the redelivered copy will settle it.
var ErrStale = errors.New("target lease lost; outcome discarded")

// DeadLetter is the envelope stored in dead-letter lists.
type DeadLetter struct {
	Payload    string `json:"payload"`
	Reason     string `json:"reason"`
	FailedAtMs int64  `json:"failed_at_ms"`
}

// Claim is a target a worker holds exclusively, together with its host lock.
type Claim struct {
	Target *models.CrawlTarget
	Raw    string
	Host   string
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

func nowMs() int64 { return time.Now().UnixMilli() }

// Route moves up to one batch of ingested targets into per-host queues.
func (f *RedisFrontier) Route(ctx context.Context) (int, error) {
	n, err := routeScript.Run(ctx, f.rdb,
		[]string{QueueFrontierIngest, FrontierHosts, FrontierScheduled, FrontierDead},
		nowMs(), routeBatchSize).Int()
	if err != nil {
		return 0, fmt.Errorf("failed to route targets: %w", err)
	}
	return n, nil
}

// Claim takes the next target whose host may be contacted now. When there is
// none it returns a nil Claim and how long until a host frees up (capped by
// the caller). Corrupt targets are dead-lettered and reported as an error.
func (f *RedisFrontier) Claim(ctx context.Context) (*Claim, time.Duration, error) {
	now := nowMs()
	res, err := claimScript.Run(ctx, f.rdb,
		[]string{FrontierHosts, QueueFrontierProcessing, FrontierLeases, FrontierScheduled},
		now, f.visibilityTimeout.Milliseconds(), f.visibilityTimeout.Milliseconds(), claimScanLimit,
	).StringSlice()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to claim target: %w", err)
	}

	switch {
	case len(res) == 0:
		return nil, time.Duration(1<<63 - 1), nil
	case res[0] == "":
		readyAt, _ := strconv.ParseFloat(res[2], 64)
		return nil, time.Duration(int64(readyAt)-now) * time.Millisecond, nil
	}

	claim := &Claim{Host: res[0], Raw: res[1]}
	var target models.CrawlTarget
	if err := json.Unmarshal([]byte(claim.Raw), &target); err != nil {
		claim.Target = &models.CrawlTarget{}
		if dlErr := f.DeadLetter(ctx, claim, "corrupted CrawlTarget JSON: "+err.Error(), 0); dlErr != nil {
			return nil, 0, dlErr
		}
		return nil, 0, fmt.Errorf("corrupted CrawlTarget JSON: %w", err)
	}
	claim.Target = &target
	return claim, 0, nil
}

func jobKey(t *models.CrawlTarget) string {
	if t == nil || t.JobID == "" {
		return "job:"
	}
	return "job:" + t.JobID
}

func (f *RedisFrontier) settle(ctx context.Context, c *Claim, hostReadyIn time.Duration, mode, dest, payloadKey string, args ...any) error {
	keys := []string{
		QueueFrontierProcessing, FrontierLeases, FrontierRedeliveries, FrontierHosts,
		dest, jobKey(c.Target), payloadKey,
	}
	argv := append([]any{c.Raw, c.Host, time.Now().Add(hostReadyIn).UnixMilli(), mode}, args...)
	committed, err := settleScript.Run(ctx, f.rdb, keys, argv...).Int()
	if err != nil {
		return fmt.Errorf("failed to settle target (%s): %w", mode, err)
	}
	if committed == 0 {
		return ErrStale
	}
	return nil
}

// PushRawPage hands the fetched page to the parser (claim-check: the payload
// goes to raw_page:<id>, the queue carries only the id) and releases the host
// for hostReadyIn. Returns the page id.
func (f *RedisFrontier) PushRawPage(ctx context.Context, c *Claim, page *models.RawPage, hostReadyIn time.Duration) (string, error) {
	payload, err := json.Marshal(page)
	if err != nil {
		return "", fmt.Errorf("failed to marshal RawPage: %w", err)
	}
	id := newPageID(page.JobID)
	err = f.settle(ctx, c, hostReadyIn, "push", QueueRawPages, RawPagePrefix+id,
		string(payload), int64(RawPageTTL.Seconds()), id)
	return id, err
}

// Finish settles a target that ends here (skipped or permanently failed).
func (f *RedisFrontier) Finish(ctx context.Context, c *Claim, hostReadyIn time.Duration) error {
	return f.settle(ctx, c, hostReadyIn, "finish", FrontierDead, "")
}

// Delay schedules nextJSON (the same or an updated target) for after delay.
func (f *RedisFrontier) Delay(ctx context.Context, c *Claim, nextJSON string, delay, hostReadyIn time.Duration) error {
	return f.settle(ctx, c, hostReadyIn, "delay", FrontierDelayed, "",
		time.Now().Add(delay).UnixMilli(), nextJSON)
}

// Release returns the target to ingest immediately, e.g. on shutdown.
func (f *RedisFrontier) Release(ctx context.Context, c *Claim) error {
	return f.settle(ctx, c, 0, "release", QueueFrontierIngest, "")
}

// DeadLetter moves a target that cannot be processed to frontier:dead.
func (f *RedisFrontier) DeadLetter(ctx context.Context, c *Claim, reason string, hostReadyIn time.Duration) error {
	envelope, err := json.Marshal(DeadLetter{Payload: c.Raw, Reason: reason, FailedAtMs: nowMs()})
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter: %w", err)
	}
	return f.settle(ctx, c, hostReadyIn, "dead", FrontierDead, "", string(envelope))
}

// IncrJobCounter bumps a per-job progress counter (job:<id> hash).
func (f *RedisFrontier) IncrJobCounter(ctx context.Context, jobID, field string) {
	if jobID == "" {
		return
	}
	_ = f.rdb.HIncrBy(ctx, "job:"+jobID, field, 1).Err()
}

// PromoteDelayed moves delayed targets that are due back to ingest.
func (f *RedisFrontier) PromoteDelayed(ctx context.Context) (int, error) {
	n, err := promoteScript.Run(ctx, f.rdb, []string{FrontierDelayed, QueueFrontierIngest},
		nowMs(), promoteBatchSize).Int()
	if err != nil {
		return 0, fmt.Errorf("failed to promote delayed targets: %w", err)
	}
	return n, nil
}

// ReapExpired re-queues (or dead-letters) targets whose lease has expired.
func (f *RedisFrontier) ReapExpired(ctx context.Context) (requeued, dead int, err error) {
	res, err := reapScript.Run(ctx, f.rdb,
		[]string{QueueFrontierProcessing, FrontierLeases, QueueFrontierIngest, FrontierDead, FrontierRedeliveries},
		nowMs(), f.visibilityTimeout.Milliseconds(), f.maxRedeliveries, "json",
	).Int64Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to reap expired leases: %w", err)
	}
	return int(res[0]), int(res[1]), nil
}

// newPageID returns "<job_id>/<random>", so scripts can find the job from the id.
func newPageID(jobID string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	if jobID == "" {
		jobID = "unknown"
	}
	return jobID + "/" + hex.EncodeToString(b[:])
}
