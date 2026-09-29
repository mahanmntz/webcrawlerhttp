"""
At-least-once consumption of the raw page queue.

The queue carries page ids ("<job_id>/<random>"); the page itself lives in
raw_page:<id> (claim-check), so queue operations never copy megabytes of HTML.

Ids move atomically from the queue to a processing list (BLMOVE) and get a
lease in a sorted set. A periodic reaper puts ids whose lease expired (their
worker died) back on the queue, and dead-letters ids that keep getting
redelivered. Settling (see pipeline.py and dead_letter below) is fenced on the
id still being in processing, so a slow worker whose id was redelivered cannot
commit twice. See shared/contracts/REDIS_SPEC.md.
"""
import hashlib
import json
import time

import redis

from app.shared_lua import JOBS_LUA, REAP_LUA

RAW_PAGE_PREFIX = "raw_page:"

# Fenced dead-letter. KEYS: processing, leases, redeliveries, dead
# ARGV: id, envelope. Returns 0 if the id was no longer ours.
DEAD_LETTER_LUA = JOBS_LUA + """
if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
  return 0
end
local lease = redis.sha1hex(ARGV[1])
redis.call('ZREM', KEYS[2], lease)
redis.call('HDEL', KEYS[3], lease)
redis.call('LPUSH', KEYS[4], ARGV[2])
finish_job(job_key_from_id(ARGV[1]))
return 1
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


def lease_id(item: str) -> str:
    return hashlib.sha1(item.encode("utf-8")).hexdigest()


class ReliableQueue:
    def __init__(
        self,
        rdb: redis.Redis,
        source: str,
        visibility_timeout_sec: int,
        max_redeliveries: int,
    ):
        self.rdb = rdb
        self.source = source
        self.processing = f"{source}:processing"
        self.leases = f"{source}:leases"
        self.dead = f"{source}:dead"
        self.redeliveries = f"{source}:redeliveries"
        self.visibility_ms = visibility_timeout_sec * 1000
        self.max_redeliveries = max_redeliveries
        self._reap = rdb.register_script(JOBS_LUA + REAP_LUA)
        self._dead_letter = rdb.register_script(DEAD_LETTER_LUA)

    @property
    def settle_keys(self) -> list[str]:
        """KEYS prefix every fenced settle script takes."""
        return [self.processing, self.leases, self.redeliveries]

    def take(self, timeout_sec: int) -> str | None:
        """Blocks up to timeout_sec for an id; returns None if none arrived."""
        item = self.rdb.blmove(self.source, self.processing, timeout_sec, "RIGHT", "LEFT")
        if item is None:
            return None
        # If we die before this, the reaper adopts the orphan.
        self.rdb.zadd(self.leases, {lease_id(item): _now_ms() + self.visibility_ms})
        return item

    def payload(self, item: str) -> str | None:
        return self.rdb.get(RAW_PAGE_PREFIX + item)

    def dead_letter(self, item: str, reason: str) -> bool:
        """Moves item to the dead-letter list and finishes its job. False if stale."""
        envelope = json.dumps({"payload": item, "reason": reason, "failed_at_ms": _now_ms()})
        return bool(self._dead_letter(keys=[*self.settle_keys, self.dead], args=[item, envelope]))

    def reap(self) -> tuple[int, int]:
        """Returns (requeued, dead_lettered) counts for expired leases."""
        requeued, dead = self._reap(
            keys=[self.processing, self.leases, self.source, self.dead, self.redeliveries],
            args=[_now_ms(), self.visibility_ms, self.max_redeliveries, "id"],
        )
        return int(requeued), int(dead)
