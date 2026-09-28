import json
import time

from app.config import Config
from app.reliable_queue import ReliableQueue
from parser_worker import Worker

GOOD_PAGE = json.dumps({
    "job_id": "job-1",
    "url": "https://example.com/",
    "status_code": 200,
    "content_type": "text/html",
    "depth": 0,
    "max_depth": 0,
    "html": "<html><body><p>hello reliable world</p></body></html>",
    "duration_ms": 10,
    "fetched_at": "2026-09-28T00:00:00Z",
})


def test_take_and_ack(rdb):
    q = ReliableQueue(rdb, "test:q", visibility_timeout_sec=60, max_redeliveries=3)
    rdb.lpush("test:q", "item")

    assert q.take(1) == "item"
    assert rdb.llen("test:q:processing") == 1
    assert rdb.zcard("test:q:leases") == 1

    q.ack("item")
    assert rdb.llen("test:q:processing") == 0
    assert rdb.zcard("test:q:leases") == 0


def test_reaper_redelivers_then_dead_letters(rdb):
    q = ReliableQueue(rdb, "test:q", visibility_timeout_sec=0, max_redeliveries=1)
    rdb.lpush("test:q", "crashy")

    # Worker takes the item and dies without acking.
    q.take(1)
    time.sleep(0.005)
    assert q.reap() == (1, 0)
    assert rdb.lrange("test:q", 0, -1) == ["crashy"]

    q.take(1)
    time.sleep(0.005)
    assert q.reap() == (0, 1)
    dead = json.loads(rdb.lindex("test:q:dead", 0))
    assert dead["payload"] == "crashy"
    assert dead["reason"] == "exceeded max redeliveries"


def test_worker_handles_page_and_acks(rdb):
    worker = Worker(rdb)
    rdb.lpush(Config.QUEUE_RAW_PAGES, GOOD_PAGE)

    worker.handle(worker.queue.take(1))

    assert rdb.llen(Config.QUEUE_PARSED_DOCS) == 1
    assert rdb.llen(worker.queue.processing) == 0


def test_worker_dead_letters_malformed_payload(rdb):
    worker = Worker(rdb)
    rdb.lpush(Config.QUEUE_RAW_PAGES, '{"url": "missing fields"}')

    worker.handle(worker.queue.take(1))

    assert rdb.llen(worker.queue.processing) == 0
    dead = json.loads(rdb.lindex(worker.queue.dead, 0))
    assert dead["reason"].startswith("malformed RawPage")


def test_worker_dead_letters_processing_bugs(rdb, monkeypatch):
    worker = Worker(rdb)
    rdb.lpush(Config.QUEUE_RAW_PAGES, GOOD_PAGE)

    def boom(_page):
        raise ValueError("parser bug")

    monkeypatch.setattr(worker.pipeline, "process_raw_page", boom)
    worker.handle(worker.queue.take(1))

    assert rdb.llen(worker.queue.processing) == 0
    assert "parser bug" in json.loads(rdb.lindex(worker.queue.dead, 0))["reason"]
