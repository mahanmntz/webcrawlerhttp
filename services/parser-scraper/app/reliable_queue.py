"""
At-least-once consumption of a Redis list.

Items move atomically from the source list to a processing list (BLMOVE) and
get a lease in a sorted set. A periodic reaper puts items whose lease expired
(their worker died) back on the source list, and dead-letters items that keep
getting redelivered. The layout is identical to the crawler's frontier; see
shared/contracts/REDIS_SPEC.md.
"""
import hashlib
import json
import time

import redis

# Same script as crawler-engine/internal/frontier/redis_frontier.go. Keep in sync.
# KEYS: processing, leases, source, dead, redeliveries
# ARGV: now_ms, visibility_ms, max_redeliveries
REAP_LUA = """
local now = tonumber(ARGV[1])
local visibility = tonumber(ARGV[2])
local max_redeliveries = tonumber(ARGV[3])
local requeued, dead = 0, 0
local present = {}

for _, item in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  local id = redis.sha1hex(item)
  present[id] = true
  local deadline = redis.call('ZSCORE', KEYS[2], id)
  if not deadline then
    redis.call('ZADD', KEYS[2], now + visibility, id)
  elseif tonumber(deadline) <= now then
    redis.call('LREM', KEYS[1], 1, item)
    redis.call('ZREM', KEYS[2], id)
    if redis.call('HINCRBY', KEYS[5], id, 1) > max_redeliveries then
      redis.call('HDEL', KEYS[5], id)
      redis.call('LPUSH', KEYS[4], cjson.encode({
        payload = item, reason = 'exceeded max redeliveries', failed_at_ms = now
      }))
      dead = dead + 1
    else
      redis.call('RPUSH', KEYS[3], item)
      requeued = requeued + 1
    end
  end
end

for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)) do
  if not present[id] then
    redis.call('ZREM', KEYS[2], id)
  end
end

return {requeued, dead}
"""


def _now_ms() -> int:
    return int(time.time() * 1000)


def item_id(item: str) -> str:
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
        self._reap = rdb.register_script(REAP_LUA)

    def take(self, timeout_sec: int) -> str | None:
        """Blocks up to timeout_sec for an item; returns None if none arrived."""
        item = self.rdb.blmove(self.source, self.processing, timeout_sec, "RIGHT", "LEFT")
        if item is None:
            return None
        # If we die before this, the reaper adopts the orphan.
        self.rdb.zadd(self.leases, {item_id(item): _now_ms() + self.visibility_ms})
        return item

    def _settle(self, item: str) -> redis.client.Pipeline:
        pipe = self.rdb.pipeline(transaction=True)
        ident = item_id(item)
        pipe.lrem(self.processing, 1, item)
        pipe.zrem(self.leases, ident)
        pipe.hdel(self.redeliveries, ident)
        return pipe

    def ack(self, item: str) -> None:
        self._settle(item).execute()

    def dead_letter(self, item: str, reason: str) -> None:
        pipe = self._settle(item)
        pipe.lpush(self.dead, json.dumps({"payload": item, "reason": reason, "failed_at_ms": _now_ms()}))
        pipe.execute()

    def reap(self) -> tuple[int, int]:
        """Returns (requeued, dead_lettered) counts for expired leases."""
        requeued, dead = self._reap(
            keys=[self.processing, self.leases, self.source, self.dead, self.redeliveries],
            args=[_now_ms(), self.visibility_ms, self.max_redeliveries],
        )
        return int(requeued), int(dead)
