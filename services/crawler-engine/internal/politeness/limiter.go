package politeness

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// acquireLua takes the host lease if it is free (SET NX PX). Otherwise it
// returns the lease's remaining TTL so the caller knows when to come back.
//
// KEYS: lease key    ARGV: worker_id, ttl_ms
// Returns -1 if acquired, else the remaining TTL in ms.
var acquireLua = redis.NewScript(`
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return -1
end
return redis.call('PTTL', KEYS[1])
`)

// Limiter coordinates distributed per-host politeness rate-limits via Redis leases.
type Limiter struct {
	rdb   *redis.Client
	delay time.Duration
}

// New creates a Limiter with a default per-host delay between requests.
func New(rdb *redis.Client, delayMs int) *Limiter {
	return &Limiter{
		rdb:   rdb,
		delay: time.Duration(delayMs) * time.Millisecond,
	}
}

// DefaultDelay is the minimum gap between two requests to the same host.
func (l *Limiter) DefaultDelay() time.Duration { return l.delay }

// AcquireLease tries to take the crawling lease for host for the given delay
// (0 means the default). It returns 0 if the lease was acquired, otherwise how
// long until the host is free again.
func (l *Limiter) AcquireLease(ctx context.Context, host, workerID string, delay time.Duration) (time.Duration, error) {
	if host == "" {
		return 0, fmt.Errorf("empty host")
	}
	if delay < l.delay {
		delay = l.delay
	}
	if delay <= 0 {
		return 0, nil
	}

	res, err := acquireLua.Run(ctx, l.rdb, []string{"politeness:host:" + host}, workerID, delay.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis error during lease acquisition: %w", err)
	}
	if res == -1 {
		return 0, nil
	}
	if res <= 0 {
		// The lease expired between SET and PTTL; retry almost immediately.
		return time.Millisecond, nil
	}
	return time.Duration(res) * time.Millisecond, nil
}
