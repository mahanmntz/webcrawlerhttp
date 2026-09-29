import { Redis } from 'ioredis';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ENQUEUE_TARGETS_LUA } from './frontier.js';
import { REPLAY_LUA } from './routes/deadLetters.js';

// Runs the real Lua scripts against a live Redis (DB 10 by default).
// Skipped when Redis is not reachable, unless REQUIRE_REDIS is set (CI).
const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  db: parseInt(process.env.REDIS_TEST_DB || '10', 10),
  lazyConnect: true,
  maxRetriesPerRequest: 0,
  retryStrategy: () => null,
});
const available = await redis.connect().then(() => true, () => false);
if (!available && process.env.REQUIRE_REDIS) {
  throw new Error('Redis required (REQUIRE_REDIS) but not reachable');
}
const keys = ['test:bloom', 'test:seen', 'test:frontier'];
const target = (job: string, url: string) =>
  JSON.stringify({ job_id: job, url, depth: 0, max_depth: 1, priority: 5, stay_in_domain: true, created_at: 'now' });

type Client = Redis & {
  enqueueTargets(...args: string[]): Promise<number[]>;
  replayDeadLetters(...args: Array<string | number>): Promise<[number, number]>;
};

describe.skipIf(!available)('ENQUEUE_TARGETS_LUA (live Redis)', () => {
  const client = redis as Client;

  beforeAll(async () => {
    client.defineCommand('enqueueTargets', { numberOfKeys: 3, lua: ENQUEUE_TARGETS_LUA });
    client.defineCommand('replayDeadLetters', { numberOfKeys: 2, lua: REPLAY_LUA });
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.flushdb();
    redis.disconnect();
  });

  it('enqueues only unseen URLs and creates the job atomically', async () => {
    const a = target('j1', 'https://a.example/');
    const b = target('j2', 'https://b.example/');
    expect(await client.enqueueTargets(...keys, '0', 'https://a.example/', a, 'https://b.example/', b)).toEqual([1, 1]);
    expect(await client.enqueueTargets(...keys, '0', 'https://a.example/', target('j3', 'https://a.example/'))).toEqual([0]);

    expect(await redis.lrange('test:frontier', 0, -1)).toEqual([b, a]);
    expect(await redis.hgetall('job:j1')).toMatchObject({
      job_id: 'j1', url: 'https://a.example/', max_depth: '1', status: 'enqueued', outstanding: '1',
    });
    expect(await redis.exists('job:j3')).toBe(0);
  });

  it('enqueues already-seen URLs when forced', async () => {
    const again = target('j4', 'https://a.example/');
    expect(await client.enqueueTargets(...keys, '1', 'https://a.example/', again)).toEqual([1]);
    expect(await redis.lindex('test:frontier', 0)).toBe(again);
  });

  it('replays frontier dead letters with attempts reset and reopens the job', async () => {
    await redis.hset('job:j9', { status: 'completed', outstanding: 0 });
    const dead = { ...JSON.parse(target('j9', 'https://c.example/')), attempts: 3 };
    await redis.lpush('test:dead', JSON.stringify({ payload: JSON.stringify(dead), reason: 'HTTP 503', failed_at_ms: 1 }));
    await redis.lpush('test:dead', 'garbage');

    expect(await client.replayDeadLetters('test:dead', 'test:replayed', 10, 'frontier', 'raw_page:')).toEqual([1, 1]);
    const replayed = JSON.parse((await redis.lindex('test:replayed', 0))!);
    expect(replayed).toMatchObject({ url: 'https://c.example/', attempts: 0 });
    expect(await redis.hgetall('job:j9')).toMatchObject({ status: 'running', outstanding: '1' });
  });

  it('replays raw page dead letters only while the payload exists', async () => {
    await redis.set('raw_page:j9/live', '{}');
    await redis.lpush('test:rawdead', JSON.stringify({ payload: 'j9/live' }), JSON.stringify({ payload: 'j9/expired' }));

    expect(await client.replayDeadLetters('test:rawdead', 'test:raw', 10, 'raw_pages', 'raw_page:')).toEqual([1, 1]);
    expect(await redis.lrange('test:raw', 0, -1)).toEqual(['j9/live']);
  });
});
