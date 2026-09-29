package frontier

// Verbatim copies of shared/redis/*.lua (Docker build contexts are per service,
// so they can't be embedded from there). TestSharedLuaMatchesCanonical fails
// if they drift.

// jobsLua is shared/redis/jobs.lua.
const jobsLua = `-- Job bookkeeping. job:<id>.outstanding counts targets of the job that are
-- enqueued but not yet finished; the job completes when it reaches zero.
local function job_key_from_json(item)
  local ok, t = pcall(cjson.decode, item)
  if ok and type(t) == 'table' and type(t.job_id) == 'string' and t.job_id ~= '' then
    return 'job:' .. t.job_id
  end
  return nil
end

-- Raw page ids are "<job_id>/<random>".
local function job_key_from_id(id)
  local job_id = string.match(id, '^([^/]+)/')
  if job_id then
    return 'job:' .. job_id
  end
  return nil
end

local function finish_job(job_key)
  if not job_key or job_key == '' or redis.call('HEXISTS', job_key, 'outstanding') == 0 then
    return
  end
  if redis.call('HINCRBY', job_key, 'outstanding', -1) <= 0 then
    local now = redis.call('TIME')
    redis.call('HSET', job_key, 'status', 'completed',
      'completed_at_ms', tostring(now[1] * 1000 + math.floor(now[2] / 1000)))
  end
end
`

// reapLua is shared/redis/reap.lua.
const reapLua = `-- Re-queues items whose worker died mid-flight.
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
`
