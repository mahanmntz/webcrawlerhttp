import { vi, describe, it, expect, afterAll } from 'vitest';
import { buildServer } from './server.js';

// In-memory mock store for isolated Redis testing without external service dependencies
const memoryStore = {
  bloom: new Set<string>(),
  sets: new Map<string, Set<string>>(),
  lists: new Map<string, string[]>(),
  hashes: new Map<string, Record<string, string>>(),
};

vi.mock('./redis.js', () => ({
  redis: {
    call: vi.fn(async (cmd: string, ...args: any[]) => {
      if (cmd === 'BF.ADD') {
        const [, item] = args;
        if (memoryStore.bloom.has(item)) return 0;
        memoryStore.bloom.add(item);
        return 1;
      }
      if (cmd === 'BF.INFO') {
        return ['Capacity', 10000, 'Size', 15000, 'Number of items inserted', memoryStore.bloom.size];
      }
      return null;
    }),
    sadd: vi.fn(async (key: string, val: string) => {
      let s = memoryStore.sets.get(key);
      if (!s) {
        s = new Set();
        memoryStore.sets.set(key, s);
      }
      if (s.has(val)) return 0;
      s.add(val);
      return 1;
    }),
    scard: vi.fn(async (key: string) => memoryStore.sets.get(key)?.size || 0),
    lrange: vi.fn(async (key: string, start: number, stop: number) => {
      const list = memoryStore.lists.get(key) || [];
      if (stop === -1) return list.slice(start);
      return list.slice(start, stop + 1);
    }),
    pipeline: vi.fn(() => {
      const ops: Array<() => [null, unknown]> = [];
      const pipe = {
        lpush: (key: string, val: string) => {
          ops.push(() => {
            let l = memoryStore.lists.get(key);
            if (!l) {
              l = [];
              memoryStore.lists.set(key, l);
            }
            l.unshift(val);
            return [null, l.length];
          });
          return pipe;
        },
        hset: (key: string, data: any) => {
          ops.push(() => {
            memoryStore.hashes.set(key, data);
            return [null, 1];
          });
          return pipe;
        },
        llen: (key: string) => {
          ops.push(() => [null, (memoryStore.lists.get(key) || []).length]);
          return pipe;
        },
        exec: async () => ops.map((op) => op()),
      };
      return pipe;
    }),
    quit: vi.fn(async () => 'OK'),
    on: vi.fn(),
  },
}));

describe('API Gateway Server Tests', () => {
  const app = buildServer();

  afterAll(async () => {
    await app.close();
  });

  it('GET /healthz returns status healthy', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/healthz',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.payload);
    expect(body.status).toBe('healthy');
    expect(body.timestamp).toBeDefined();
  });

  it('POST /api/jobs rejects request with missing or invalid URL', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs',
      payload: { max_depth: 2 },
    });

    expect(response.statusCode).toBe(400);
  });

  it('POST /api/jobs/batch successfully ingests array of URLs and handles deduplication', async () => {
    const uniqueUrl = `https://test-batch-${Date.now()}.org`;
    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs/batch',
      payload: {
        urls: [uniqueUrl, 'https://invalid-url-filtered', uniqueUrl], // Includes duplicate and invalid
        max_depth: 2,
        priority: 6,
      },
    });

    expect(response.statusCode).toBe(201);
    const body = JSON.parse(response.payload);
    expect(body.enqueued_count).toBeGreaterThanOrEqual(1);
    expect(body.enqueued_urls).toContain(uniqueUrl);
    expect(body.job_ids.length).toBe(body.enqueued_count);
  });

  it('GET /api/documents/export returns exported documents array', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/api/documents/export',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.payload);
    expect(body.exported_at).toBeDefined();
    expect(Array.isArray(body.documents)).toBe(true);
    expect(typeof body.total_count).toBe('number');
  });

  it('GET / serves the Web UI dashboard', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/',
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/html');
    expect(response.payload).toContain('SpiderRAG');
  });
});
