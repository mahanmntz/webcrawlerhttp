-- Marks each URL as seen and, if it is new (or force is set), pushes its
-- CrawlTarget onto the frontier, in one atomic step. Uses RedisBloom when
-- loaded, otherwise an exact Set. args[first..] are (canonical_url, target_json)
-- pairs. With record_job, the job hash is created from the target (seeds).
-- Returns one 1/0 per pair: 1 if enqueued.
local function enqueue_targets(bloom, seen_set, frontier, force, record_job, args, first)
  if redis.call('EXISTS', bloom) == 0 then
    redis.pcall('BF.RESERVE', bloom, '0.001', '1000000')
  end

  local results = {}
  for i = first, #args, 2 do
    local url, target = args[i], args[i + 1]
    local added = redis.pcall('BF.ADD', bloom, url)
    if type(added) == 'table' and added.err then
      added = redis.call('SADD', seen_set, url)
    end
    if added == 1 or force then
      redis.call('LPUSH', frontier, target)
      local t = cjson.decode(target)
      local job = 'job:' .. t.job_id
      if record_job then
        redis.call('HSET', job, 'job_id', t.job_id, 'url', t.url,
          'max_depth', tostring(t.max_depth), 'priority', tostring(t.priority),
          'stay_in_domain', tostring(t.stay_in_domain), 'created_at', t.created_at)
        redis.call('HSETNX', job, 'status', 'enqueued')
      elseif redis.call('HGET', job, 'status') == 'completed' then
        redis.call('HSET', job, 'status', 'running')
      end
      redis.call('HINCRBY', job, 'outstanding', 1)
      results[#results + 1] = 1
    else
      results[#results + 1] = 0
    end
  end
  return results
end
