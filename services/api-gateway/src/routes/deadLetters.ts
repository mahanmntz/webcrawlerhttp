import { FastifyPluginAsync } from 'fastify';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { isAdmin } from '../auth.js';

type DeadLetterQueue = 'frontier' | 'raw_pages';

const QUEUES: Record<DeadLetterQueue, { dead: string; target: string }> = {
  frontier: { dead: config.frontierDead, target: config.queueFrontier },
  raw_pages: { dead: config.rawPagesDead, target: config.queueRawPages },
};

/**
 * Moves the oldest dead letters back to their queue. Crawl targets get their
 * attempts reset; raw pages are replayed only if their payload still exists.
 * Each replayed item reopens its job (outstanding + 1).
 *
 * KEYS: dead list, target queue
 * ARGV: count, kind ('frontier' | 'raw_pages'), raw page key prefix
 * Returns {replayed, dropped}.
 */
export const REPLAY_LUA = `
local replayed, dropped = 0, 0
for _ = 1, tonumber(ARGV[1]) do
  local envelope = redis.call('RPOP', KEYS[1])
  if not envelope then
    break
  end
  local ok, e = pcall(cjson.decode, envelope)
  local payload = ok and type(e) == 'table' and e.payload or nil
  local requeued, job_id = false, nil

  if type(payload) == 'string' and ARGV[2] == 'frontier' then
    local ok_target, t = pcall(cjson.decode, payload)
    if ok_target and type(t) == 'table' and type(t.url) == 'string' then
      t.attempts = 0
      redis.call('LPUSH', KEYS[2], cjson.encode(t))
      requeued, job_id = true, t.job_id
    end
  elseif type(payload) == 'string' and redis.call('EXISTS', ARGV[3] .. payload) == 1 then
    redis.call('LPUSH', KEYS[2], payload)
    requeued, job_id = true, string.match(payload, '^([^/]+)/')
  end

  if not requeued then
    -- Unparseable, or a raw page whose payload has expired.
    dropped = dropped + 1
  else
    replayed = replayed + 1
    if type(job_id) == 'string' and job_id ~= '' then
      local job = 'job:' .. job_id
      redis.call('HINCRBY', job, 'outstanding', 1)
      if redis.call('HGET', job, 'status') == 'completed' then
        redis.call('HSET', job, 'status', 'running')
      end
    end
  end
end
return {replayed, dropped}
`;

let commandDefined = false;
type ReplayClient = typeof redis & {
  replayDeadLetters(dead: string, target: string, count: number, kind: string, prefix: string): Promise<[number, number]>;
};

function client(): ReplayClient {
  if (!commandDefined) {
    redis.defineCommand('replayDeadLetters', { numberOfKeys: 2, lua: REPLAY_LUA });
    commandDefined = true;
  }
  return redis as ReplayClient;
}

const queueSchema = { type: 'string', enum: Object.keys(QUEUES), default: 'frontier' } as const;

export const deadLetterRoutes: FastifyPluginAsync = async (fastify) => {
  // Inspect the newest dead letters of a queue.
  fastify.get<{ Querystring: { queue: DeadLetterQueue; limit: number } }>(
    '/api/dead-letters',
    {
      schema: {
        querystring: {
          type: 'object',
          properties: { queue: queueSchema, limit: { type: 'integer', minimum: 1, maximum: 200, default: 20 } },
        },
      },
    },
    async (request, reply) => {
      const { queue, limit } = request.query;
      const { dead } = QUEUES[queue];
      const [total, items] = await Promise.all([redis.llen(dead), redis.lrange(dead, 0, limit - 1)]);

      const deadLetters = items.map((item) => {
        try {
          return JSON.parse(item);
        } catch {
          return { payload: item, reason: 'unparseable dead letter' };
        }
      });
      return reply.send({ queue, total, dead_letters: deadLetters });
    }
  );

  // Replay the oldest dead letters (e.g. after fixing the cause).
  fastify.post<{ Body: { queue: DeadLetterQueue; count: number } }>(
    '/api/dead-letters/replay',
    {
      schema: {
        body: {
          type: 'object',
          properties: { queue: queueSchema, count: { type: 'integer', minimum: 1, maximum: 1000, default: 100 } },
        },
      },
    },
    async (request, reply) => {
      if (!isAdmin(request.headers)) {
        return reply.status(401).send({ error: 'Missing or invalid x-admin-token header' });
      }
      const { queue, count } = request.body ?? { queue: 'frontier', count: 100 };
      const { dead, target } = QUEUES[queue];
      const [replayed, dropped] = await client().replayDeadLetters(dead, target, count, queue, config.rawPagePrefix);
      return reply.send({ queue, replayed: Number(replayed), dropped: Number(dropped) });
    }
  );
};
