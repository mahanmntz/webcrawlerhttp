package frontier

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"crawler-engine/internal/testredis"
)

const testDB = 15

func push(t *testing.T, f *RedisFrontier, payloads ...string) {
	t.Helper()
	for _, p := range payloads {
		if err := f.rdb.LPush(context.Background(), QueueFrontierPending, p).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func llen(t *testing.T, f *RedisFrontier, key string) int64 {
	t.Helper()
	n, err := f.rdb.LLen(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestDequeueAcknowledge(t *testing.T) {
	ctx := context.Background()
	f := New(testredis.Client(t, testDB), time.Minute, 3)
	push(t, f, `{"job_id":"j1","url":"https://a.example/","depth":0,"max_depth":1,"priority":5,"created_at":"2026-01-01T00:00:00Z"}`)

	target, raw, err := f.DequeueReliable(ctx, time.Second)
	if err != nil || target == nil {
		t.Fatalf("dequeue: target=%v err=%v", target, err)
	}
	if llen(t, f, QueueFrontierProcessing) != 1 {
		t.Fatal("expected target in processing")
	}
	if n, _ := f.rdb.ZCard(ctx, FrontierLeases).Result(); n != 1 {
		t.Fatal("expected a lease")
	}

	if err := f.Acknowledge(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if llen(t, f, QueueFrontierProcessing) != 0 {
		t.Fatal("expected processing to be empty after ack")
	}
	if n, _ := f.rdb.ZCard(ctx, FrontierLeases).Result(); n != 0 {
		t.Fatal("expected lease to be removed after ack")
	}
}

func TestDequeue_CorruptPayloadIsDeadLettered(t *testing.T) {
	ctx := context.Background()
	f := New(testredis.Client(t, testDB), time.Minute, 3)
	push(t, f, "{not json")

	if _, _, err := f.DequeueReliable(ctx, time.Second); err == nil {
		t.Fatal("expected an error for corrupt JSON")
	}
	if llen(t, f, QueueFrontierProcessing) != 0 || llen(t, f, FrontierDead) != 1 {
		t.Fatal("expected corrupt payload to move from processing to dead letter")
	}

	var dl DeadLetter
	raw, _ := f.rdb.LIndex(ctx, FrontierDead, 0).Result()
	if err := json.Unmarshal([]byte(raw), &dl); err != nil || dl.Payload != "{not json" {
		t.Fatalf("unexpected dead letter %q: %v", raw, err)
	}
}

func TestReaper_RequeuesStalledTargetsThenDeadLetters(t *testing.T) {
	ctx := context.Background()
	// Zero visibility: every lease is expired as soon as it is registered.
	f := New(testredis.Client(t, testDB), 0, 1)
	payload := `{"job_id":"j1","url":"https://a.example/","depth":0,"max_depth":1,"priority":5,"created_at":"2026-01-01T00:00:00Z"}`
	push(t, f, payload)

	// Worker dequeues then "crashes" without settling.
	if _, _, err := f.DequeueReliable(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)

	requeued, dead, err := f.ReapExpired(ctx)
	if err != nil || requeued != 1 || dead != 0 {
		t.Fatalf("first reap: requeued=%d dead=%d err=%v", requeued, dead, err)
	}
	if llen(t, f, QueueFrontierPending) != 1 || llen(t, f, QueueFrontierProcessing) != 0 {
		t.Fatal("expected target back on the frontier")
	}

	// Crash again: this exceeds MaxRedeliveries=1.
	if _, _, err := f.DequeueReliable(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	requeued, dead, err = f.ReapExpired(ctx)
	if err != nil || requeued != 0 || dead != 1 {
		t.Fatalf("second reap: requeued=%d dead=%d err=%v", requeued, dead, err)
	}
	if llen(t, f, FrontierDead) != 1 {
		t.Fatal("expected target in dead letter")
	}
}

func TestReaper_AdoptsOrphansWithoutLease(t *testing.T) {
	ctx := context.Background()
	f := New(testredis.Client(t, testDB), time.Minute, 3)

	// Simulate a worker that died between BLMOVE and registering its lease.
	if err := f.rdb.LPush(ctx, QueueFrontierProcessing, `{"url":"orphan"}`).Err(); err != nil {
		t.Fatal(err)
	}

	requeued, _, err := f.ReapExpired(ctx)
	if err != nil || requeued != 0 {
		t.Fatalf("orphan must get a fresh lease, not be requeued immediately: requeued=%d err=%v", requeued, err)
	}
	if n, _ := f.rdb.ZCard(ctx, FrontierLeases).Result(); n != 1 {
		t.Fatal("expected orphan to be given a lease")
	}
}

func TestDelay_PromotesWhenDue(t *testing.T) {
	ctx := context.Background()
	f := New(testredis.Client(t, testDB), time.Minute, 3)
	push(t, f, `{"url":"https://a.example/"}`)

	_, raw, err := f.DequeueReliable(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Delay(ctx, raw, `{"url":"https://a.example/","attempts":1}`, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if llen(t, f, QueueFrontierProcessing) != 0 {
		t.Fatal("delayed target must leave processing")
	}

	if n, _ := f.PromoteDelayed(ctx); n != 0 {
		t.Fatal("target promoted before it was due")
	}
	time.Sleep(60 * time.Millisecond)
	if n, _ := f.PromoteDelayed(ctx); n != 1 {
		t.Fatal("expected target to be promoted once due")
	}
	got, _ := f.rdb.LIndex(ctx, QueueFrontierPending, -1).Result()
	if got != `{"url":"https://a.example/","attempts":1}` {
		t.Fatalf("expected updated payload at the head of the frontier, got %q", got)
	}
}
