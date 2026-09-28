package politeness

import (
	"context"
	"testing"
	"time"

	"crawler-engine/internal/testredis"
)

func TestAcquireLease_ReturnsRemainingWait(t *testing.T) {
	ctx := context.Background()
	l := New(testredis.Client(t, 13), 1000)

	wait, err := l.AcquireLease(ctx, "example.com", "w1", 0)
	if err != nil || wait != 0 {
		t.Fatalf("first acquire: wait=%v err=%v", wait, err)
	}

	wait, err = l.AcquireLease(ctx, "example.com", "w2", 0)
	if err != nil || wait <= 0 || wait > time.Second {
		t.Fatalf("second acquire should report remaining wait: wait=%v err=%v", wait, err)
	}

	// Other hosts are independent.
	if wait, _ := l.AcquireLease(ctx, "other.example", "w2", 0); wait != 0 {
		t.Fatalf("other host should be free, got wait=%v", wait)
	}
}

func TestAcquireLease_CrawlDelayExtendsLease(t *testing.T) {
	ctx := context.Background()
	l := New(testredis.Client(t, 13), 100)

	if _, err := l.AcquireLease(ctx, "slow.example", "w1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	wait, _ := l.AcquireLease(ctx, "slow.example", "w2", 0)
	if wait < 4*time.Second {
		t.Fatalf("expected Crawl-delay to hold the host for ~5s, got %v", wait)
	}
}

func TestAcquireLease_RejectsEmptyHost(t *testing.T) {
	l := New(testredis.Client(t, 13), 100)
	if _, err := l.AcquireLease(context.Background(), "", "w1", 0); err == nil {
		t.Fatal("expected error for empty host")
	}
}
