-- Re-queues items whose worker died mid-flight.
--
-- Every item in the processing list has a lease in a sorted set, keyed by the
-- SHA-1 of the item and scored by its deadline (ms). Items without a lease
-- (the worker died between taking it and registering the lease) get one now.
-- Items past their deadline go back to the source queue, or to the dead-letter
-- list once they have been redelivered more than max_redeliveries times; a
-- dead-lettered item finishes its job.
--
-- KEYS: processing, leases, source, dead, redeliveries
-- ARGV: now_ms, visibility_ms, max_redeliveries, job_id_mode ('json' | 'id')
local now = tonumber(ARGV[1])
local visibility = tonumber(ARGV[2])
local max_redeliveries = tonumber(ARGV[3])
local job_key_of = job_key_from_json
if ARGV[4] == 'id' then
  job_key_of = job_key_from_id
end
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
      finish_job(job_key_of(item))
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
