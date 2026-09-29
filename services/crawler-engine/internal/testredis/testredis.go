// Package testredis connects tests to a real Redis at REDIS_TEST_ADDR
// (default localhost:6379). Tests are skipped when it is unreachable, unless
// REQUIRE_REDIS is set (CI), in which case they fail.
package testredis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client returns a client on the given logical DB, flushed before and after
// the test. Give each package its own DB: `go test ./...` runs packages in
// parallel.
func Client(t *testing.T, db int) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: db})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		if os.Getenv("REQUIRE_REDIS") != "" {
			t.Fatalf("Redis required but not reachable at %s: %v", addr, err)
		}
		t.Skipf("Redis not reachable at %s: %v", addr, err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush test DB: %v", err)
	}

	t.Cleanup(func() {
		_ = rdb.FlushDB(context.Background()).Err()
		_ = rdb.Close()
	})
	return rdb
}
