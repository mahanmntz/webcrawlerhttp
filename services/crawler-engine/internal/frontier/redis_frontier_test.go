package frontier

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"crawler-engine/internal/models"
	"crawler-engine/internal/testredis"
)

const testDB = 15

func newFrontier(t *testing.T, visibility time.Duration, maxRedeliveries int) *RedisFrontier {
	t.Helper()
	return New(testredis.Client(t, testDB), visibility, maxRedeliveries)
}

func target(jobID, url string, priority int) string {
	b, _ := json.Marshal(models.CrawlTarget{JobID: jobID, URL: url, MaxDepth: 1, Priority: priority, CreatedAt: time.Now()})
	return string(b)
}

// ingest pushes targets and routes them to host queues, as producers + the
// maintenance loop would. Jobs get an outstanding count like the gateway sets.
func ingest(t *testing.T, f *RedisFrontier, payloads ...string) {
	t.Helper()
	ctx := context.Background()
	for _, p := range payloads {
		var tgt models.CrawlTarget
		_ = json.Unmarshal([]byte(p), &tgt)
		if tgt.JobID != "" {
			f.rdb.HSetNX(ctx, "job:"+tgt.JobID, "status", "enqueued")
			f.rdb.HIncrBy(ctx, "job:"+tgt.JobID, "outstanding", 1)
		}
		if err := f.rdb.LPush(ctx, QueueFrontierIngest, p).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Route(ctx); err != nil {
		t.Fatal(err)
	}
}

func mustClaim(t *testing.T, f *RedisFrontier) *Claim {
	t.Helper()
	c, _, err := f.Claim(context.Background())
	if err != nil || c == nil {
		t.Fatalf("expected a claim, got %v err=%v", c, err)
	}
	return c
}

func llen(t *testing.T, f *RedisFrontier, key string) int64 {
	t.Helper()
	n, _ := f.rdb.LLen(context.Background(), key).Result()
	return n
}

func jobField(f *RedisFrontier, jobID, field string) string {
	v, _ := f.rdb.HGet(context.Background(), "job:"+jobID, field).Result()
	return v
}

func TestSharedLuaMatchesCanonical(t *testing.T) {
	for file, embedded := range map[string]string{"jobs.lua": jobsLua, "reap.lua": reapLua} {
		canonical, err := os.ReadFile("../../../../shared/redis/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(canonical)) != strings.TrimSpace(embedded) {
			t.Errorf("embedded %s differs from shared/redis/%s", file, file)
		}
	}
}

func TestClaim_RoutesAndLocksHost(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, time.Minute, 3)
	ingest(t, f,
		target("j1", "https://a.example/1", 5),
		target("j1", "https://a.example/2", 5),
		target("j1", "https://b.example/1", 5),
	)
	if n, _ := f.rdb.Get(ctx, FrontierScheduled).Int(); n != 3 {
		t.Fatalf("expected 3 scheduled, got %d", n)
	}

	first := mustClaim(t, f)
	second := mustClaim(t, f)
	if first.Host == second.Host {
		t.Fatalf("one host must not be claimed twice while locked: %s", first.Host)
	}
	if c, wait, _ := f.Claim(ctx); c != nil || wait <= 0 {
		t.Fatalf("both hosts are locked, expected nothing and a wait; got %v wait=%v", c, wait)
	}
	if jobField(f, "j1", "status") != "running" {
		t.Fatal("claiming a target should mark its job running")
	}

	// Settling with a delay keeps the host closed for that long.
	host := first.Host
	if err := f.Finish(ctx, first, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if c, _, _ := f.Claim(ctx); c != nil {
		t.Fatalf("host %s claimed before its politeness delay elapsed", c.Host)
	}
	time.Sleep(60 * time.Millisecond)
	if c := mustClaim(t, f); c.Host != host {
		t.Fatalf("expected %s to be free again, got %s", host, c.Host)
	}
}

func TestClaim_HigherPriorityFirstWithinHost(t *testing.T) {
	f := newFrontier(t, time.Minute, 3)
	ingest(t, f, target("j1", "https://a.example/low", 2), target("j1", "https://a.example/high", 9))

	if c := mustClaim(t, f); c.Target.URL != "https://a.example/high" {
		t.Fatalf("expected the priority 9 target first, got %s", c.Target.URL)
	}
}

func TestRoute_UnroutableTargetIsDeadLetteredAndFinished(t *testing.T) {
	f := newFrontier(t, time.Minute, 3)
	ingest(t, f, target("j1", "ftp://files.example/x", 5))

	if llen(t, f, FrontierDead) != 1 {
		t.Fatal("expected unroutable target in dead letter")
	}
	if jobField(f, "j1", "status") != "completed" {
		t.Fatal("a job whose only target is dead-lettered should complete")
	}
}

func TestPushRawPage_ClaimCheck(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, time.Minute, 3)
	ingest(t, f, target("j1", "https://a.example/", 5))
	c := mustClaim(t, f)

	id, err := f.PushRawPage(ctx, c, &models.RawPage{JobID: "j1", URL: "https://a.example/", HTML: "<p>hi</p>"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "j1/") {
		t.Fatalf("page id must start with the job id, got %q", id)
	}
	if got, _ := f.rdb.LIndex(ctx, QueueRawPages, 0).Result(); got != id {
		t.Fatalf("queue must carry only the id, got %q", got)
	}
	payload, _ := f.rdb.Get(ctx, RawPagePrefix+id).Result()
	var stored models.RawPage
	if json.Unmarshal([]byte(payload), &stored) != nil || stored.HTML != "<p>hi</p>" {
		t.Fatalf("payload not stored under raw_page:<id>: %q", payload)
	}
	if ttl, _ := f.rdb.TTL(ctx, RawPagePrefix+id).Result(); ttl <= 0 {
		t.Fatal("raw page payload must expire")
	}
	if llen(t, f, QueueFrontierProcessing) != 0 {
		t.Fatal("pushing must settle the target")
	}
	if jobField(f, "j1", "outstanding") != "1" {
		t.Fatal("a page handed to the parser is still outstanding until parsed")
	}
}

func TestSettle_IsFencedOnTheLease(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, 0, 3) // leases expire immediately
	ingest(t, f, target("j1", "https://a.example/", 5))
	stale := mustClaim(t, f)
	time.Sleep(5 * time.Millisecond)

	// The reaper hands the target back while the first worker is still busy.
	if requeued, _, _ := f.ReapExpired(ctx); requeued != 1 {
		t.Fatal("expected the target to be reaped")
	}

	if _, err := f.PushRawPage(ctx, stale, &models.RawPage{JobID: "j1"}, 0); !errors.Is(err, ErrStale) {
		t.Fatalf("expected ErrStale for a settle after the lease was lost, got %v", err)
	}
	if llen(t, f, QueueRawPages) != 0 {
		t.Fatal("a stale outcome must not have side effects")
	}
}

func TestFinish_CompletesJobWhenNothingIsOutstanding(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, time.Minute, 3)
	ingest(t, f, target("j1", "https://a.example/1", 5), target("j1", "https://b.example/1", 5))

	if err := f.Finish(ctx, mustClaim(t, f), 0); err != nil {
		t.Fatal(err)
	}
	if jobField(f, "j1", "status") == "completed" {
		t.Fatal("job completed while a target is still outstanding")
	}
	if err := f.DeadLetter(ctx, mustClaim(t, f), "gone", 0); err != nil {
		t.Fatal(err)
	}
	if jobField(f, "j1", "status") != "completed" || jobField(f, "j1", "completed_at_ms") == "" {
		t.Fatal("expected job completed with a timestamp")
	}
}

func TestClaim_CorruptPayloadIsDeadLettered(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, time.Minute, 3)
	// Routable (the router only needs the url) but not a valid CrawlTarget.
	ingest(t, f, `{"url":"https://a.example/","depth":"deep"}`)

	if _, _, err := f.Claim(ctx); err == nil {
		t.Fatal("expected an error for a corrupt target")
	}
	if llen(t, f, QueueFrontierProcessing) != 0 || llen(t, f, FrontierDead) != 1 {
		t.Fatal("expected corrupt payload to move from processing to dead letter")
	}
}

func TestReaper_RequeuesThenDeadLettersAndFinishesJob(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, 0, 1)
	ingest(t, f, target("j1", "https://a.example/", 5))

	// Worker claims then "crashes" without settling.
	mustClaim(t, f)
	time.Sleep(5 * time.Millisecond)
	if requeued, dead, err := f.ReapExpired(ctx); err != nil || requeued != 1 || dead != 0 {
		t.Fatalf("first reap: requeued=%d dead=%d err=%v", requeued, dead, err)
	}
	if llen(t, f, QueueFrontierIngest) != 1 {
		t.Fatal("expected target back on ingest")
	}

	// Crash again: this exceeds MaxRedeliveries=1. The host lock (0ms) is free.
	if _, err := f.Route(ctx); err != nil {
		t.Fatal(err)
	}
	mustClaim(t, f)
	time.Sleep(5 * time.Millisecond)
	if requeued, dead, err := f.ReapExpired(ctx); err != nil || requeued != 0 || dead != 1 {
		t.Fatalf("second reap: requeued=%d dead=%d err=%v", requeued, dead, err)
	}
	if jobField(f, "j1", "status") != "completed" {
		t.Fatal("dead-lettering the last target should complete the job")
	}
}

func TestReaper_AdoptsOrphansWithoutLease(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, time.Minute, 3)
	if err := f.rdb.LPush(ctx, QueueFrontierProcessing, `{"url":"orphan"}`).Err(); err != nil {
		t.Fatal(err)
	}

	if requeued, _, err := f.ReapExpired(ctx); err != nil || requeued != 0 {
		t.Fatalf("orphan must get a fresh lease, not be requeued immediately: requeued=%d err=%v", requeued, err)
	}
	if n, _ := f.rdb.ZCard(ctx, FrontierLeases).Result(); n != 1 {
		t.Fatal("expected orphan to be given a lease")
	}
}

func TestDelay_PromotesWhenDue(t *testing.T) {
	ctx := context.Background()
	f := newFrontier(t, time.Minute, 3)
	ingest(t, f, target("j1", "https://a.example/", 5))
	c := mustClaim(t, f)

	if err := f.Delay(ctx, c, `{"url":"https://a.example/","attempts":1}`, 50*time.Millisecond, 0); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.PromoteDelayed(ctx); n != 0 {
		t.Fatal("target promoted before it was due")
	}
	time.Sleep(60 * time.Millisecond)
	if n, _ := f.PromoteDelayed(ctx); n != 1 {
		t.Fatal("expected target to be promoted once due")
	}
	if got, _ := f.rdb.LIndex(ctx, QueueFrontierIngest, -1).Result(); got != `{"url":"https://a.example/","attempts":1}` {
		t.Fatalf("expected updated payload on ingest, got %q", got)
	}
}
