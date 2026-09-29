import json
import time

from app.config import Config
from app.reliable_queue import RAW_PAGE_PREFIX, ReliableQueue
from parser_worker import Worker

GOOD_PAGE = json.dumps({
    "job_id": "job-1",
    "url": "https://example.com/",
    "status_code": 200,
    "content_type": "text/html",
    "depth": 0,
    "max_depth": 1,
    "html": '<html><body><p>hello reliable world</p><a href="/next">next</a></body></html>',
    "duration_ms": 10,
    "fetched_at": "2026-09-28T00:00:00Z",
})


def enqueue_page(rdb, page_id="job-1/abc", payload=GOOD_PAGE, outstanding=1):
    """What the crawler does: payload under raw_page:<id>, id on the queue."""
    rdb.hset("job:job-1", mapping={"status": "running", "outstanding": outstanding})
    rdb.set(RAW_PAGE_PREFIX + page_id, payload)
    rdb.lpush(Config.QUEUE_RAW_PAGES, page_id)


def test_take_registers_lease(rdb):
    q = ReliableQueue(rdb, "test:q", visibility_timeout_sec=60, max_redeliveries=3)
    rdb.lpush("test:q", "job-1/x")

    assert q.take(1) == "job-1/x"
    assert rdb.llen("test:q:processing") == 1
    assert rdb.zcard("test:q:leases") == 1


def test_reaper_redelivers_then_dead_letters_and_finishes_job(rdb):
    q = ReliableQueue(rdb, "test:q", visibility_timeout_sec=0, max_redeliveries=1)
    rdb.hset("job:job-1", mapping={"status": "running", "outstanding": 1})
    rdb.lpush("test:q", "job-1/crashy")

    # Worker takes the id and dies without settling.
    q.take(1)
    time.sleep(0.005)
    assert q.reap() == (1, 0)
    assert rdb.lrange("test:q", 0, -1) == ["job-1/crashy"]

    q.take(1)
    time.sleep(0.005)
    assert q.reap() == (0, 1)
    dead = json.loads(rdb.lindex("test:q:dead", 0))
    assert dead["payload"] == "job-1/crashy"
    assert dead["reason"] == "exceeded max redeliveries"
    assert rdb.hget("job:job-1", "status") == "completed"


def test_worker_processes_page_and_acknowledges_in_one_commit(rdb):
    worker = Worker(rdb)
    enqueue_page(rdb)

    worker.handle(worker.queue.take(1))

    assert rdb.llen(Config.QUEUE_PARSED_DOCS) == 1
    assert rdb.llen(worker.queue.processing) == 0
    assert rdb.zcard(worker.queue.leases) == 0
    assert not rdb.exists(RAW_PAGE_PREFIX + "job-1/abc"), "payload must be deleted once parsed"
    # One page finished, one child enqueued: the job is still running.
    assert rdb.hget("job:job-1", "outstanding") == "1"
    assert rdb.hget("job:job-1", "status") == "running"


def test_worker_stale_page_is_not_committed_twice(rdb):
    worker = Worker(rdb)
    enqueue_page(rdb)
    page_id = worker.queue.take(1)

    # The reaper handed the page to another worker, which committed it first.
    rdb.lrem(worker.queue.processing, 1, page_id)
    worker.handle(page_id)

    assert rdb.llen(Config.QUEUE_PARSED_DOCS) == 0
    assert rdb.hget("job:job-1", "outstanding") == "1"


def test_worker_dead_letters_malformed_payload(rdb):
    worker = Worker(rdb)
    enqueue_page(rdb, payload='{"url": "missing fields"}')

    worker.handle(worker.queue.take(1))

    assert rdb.llen(worker.queue.processing) == 0
    dead = json.loads(rdb.lindex(worker.queue.dead, 0))
    assert dead["payload"] == "job-1/abc"
    assert dead["reason"].startswith("malformed RawPage")
    assert rdb.hget("job:job-1", "status") == "completed"


def test_worker_dead_letters_missing_payload(rdb):
    worker = Worker(rdb)
    enqueue_page(rdb)
    rdb.delete(RAW_PAGE_PREFIX + "job-1/abc")  # expired

    worker.handle(worker.queue.take(1))

    assert "missing or expired" in json.loads(rdb.lindex(worker.queue.dead, 0))["reason"]


def test_worker_dead_letters_processing_bugs(rdb, monkeypatch):
    worker = Worker(rdb)
    enqueue_page(rdb)

    def boom(*_args, **_kwargs):
        raise ValueError("parser bug")

    monkeypatch.setattr(worker.pipeline, "process_raw_page", boom)
    worker.handle(worker.queue.take(1))

    assert rdb.llen(worker.queue.processing) == 0
    assert "parser bug" in json.loads(rdb.lindex(worker.queue.dead, 0))["reason"]
