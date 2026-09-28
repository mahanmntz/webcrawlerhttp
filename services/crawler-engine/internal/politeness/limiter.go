package politeness

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter coordinates distributed per-host politeness rate-limits via Redis leases.
type Limiter struct {
	rdb     *redis.Client
	delayMs int
}

// New creates a Limiter instance.
func New(rdb *redis.Client, delayMs int) *Limiter {
	return &Limiter{
		rdb:     rdb,
		delayMs: delayMs,
	}
}

// AcquireLease attempts to acquire an exclusive crawling lease for a specific host.
// Returns true if the lease was acquired, false if the host is cooling down.
func (l *Limiter) AcquireLease(ctx context.Context, targetURL string, workerID string) (bool, string, error) {
	parsed, err := url.Parse(targetURL)
	if err != nil {
		return false, "", fmt.Errorf("invalid url format: %w", err)
	}

	host := parsed.Hostname()
	if host == "" {
		return false, "", fmt.Errorf("empty host extracted from url: %s", targetURL)
	}

	leaseKey := fmt.Sprintf("politeness:host:%s", host)
	ttl := time.Duration(l.delayMs) * time.Millisecond

	// SET politeness:host:<domain> <workerID> NX PX <ttl>
	acquired, err := l.rdb.SetNX(ctx, leaseKey, workerID, ttl).Result()
	if err != nil {
		return false, host, fmt.Errorf("redis error during lease acquisition: %w", err)
	}

	return acquired, host, nil
}
