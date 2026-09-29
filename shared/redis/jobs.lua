-- Job bookkeeping. job:<id>.outstanding counts targets of the job that are
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
