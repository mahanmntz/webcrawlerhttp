import { redis } from './redis.js';
import { config } from './config.js';
import { CrawlTarget } from './types.js';

/**
 * shared/redis/enqueue_targets.lua, verbatim (the Docker build context is this
 * service only). frontier.test.ts fails if it drifts from the canonical file.
 */
export const ENQUEUE_TARGETS_FN = `-- Marks each URL as seen and, if it is new (or force is set), pushes its
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
`;

/**
 * Atomically marks each seed URL as seen and, if it was new (or force is set),
 * pushes its CrawlTarget onto the frontier and creates its job hash. Doing it
 * in one script means a URL is never marked seen without being enqueued, and
 * the job exists before any worker can touch it.
 *
 * KEYS: bloom filter, seen set, frontier queue
 * ARGV: force ("1"/"0"), then (canonical_url, target_json) pairs
 * Returns one 1/0 per pair: 1 if enqueued.
 */
export const ENQUEUE_TARGETS_LUA = `${ENQUEUE_TARGETS_FN}
return enqueue_targets(KEYS[1], KEYS[2], KEYS[3], ARGV[1] == '1', true, ARGV, 2)
`;

type EnqueueClient = typeof redis & {
  enqueueTargets(bloom: string, seenSet: string, frontier: string, ...args: string[]): Promise<number[]>;
};

let commandDefined = false;

function client(): EnqueueClient {
  if (!commandDefined) {
    redis.defineCommand('enqueueTargets', { numberOfKeys: 3, lua: ENQUEUE_TARGETS_LUA });
    commandDefined = true;
  }
  return redis as EnqueueClient;
}

/** Returns, for each target, whether it was enqueued (false = already seen). */
export async function enqueueTargets(targets: CrawlTarget[], force: boolean): Promise<boolean[]> {
  if (targets.length === 0) return [];

  const args = [force ? '1' : '0'];
  for (const target of targets) {
    args.push(target.url, JSON.stringify(target));
  }

  const results = await client().enqueueTargets(
    config.bloomUrlSeen,
    config.setSeenUrls,
    config.queueFrontier,
    ...args,
  );
  return results.map((r) => Number(r) === 1);
}
