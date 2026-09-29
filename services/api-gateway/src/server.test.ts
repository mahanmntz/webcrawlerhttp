import { vi, describe, it, expect, afterAll } from 'vitest';
import { buildServer } from './server.js';
import { parseBloomInfo } from './routes/metrics.js';
import { readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { Ajv } from 'ajv';
import ajvFormats from 'ajv-formats';

// ajv-formats is CommonJS; under NodeNext its default export is the module.
const addFormats = ajvFormats as unknown as typeof ajvFormats.default;

const contractsDir = path.join(path.dirname(fileURLToPath(import.meta.url)), '../../../shared/contracts');
const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCrawlTarget = ajv.compile(JSON.parse(readFileSync(path.join(contractsDir, 'crawl_target.json'), 'utf-8')));

// In-memory mock store for isolated Redis testing without external service dependencies
const memoryStore = {
  bloom: new Set<string>(),
  sets: new Map<string, Set<string>>(),
  lists: new Map<string, string[]>(),
  hashes: new Map<string, Record<string, string>>(),
};

vi.mock('./redis.js', () => {
  const redis: Record<string, any> = {
    // Mirrors ENQUEUE_TARGETS_LUA using the exact-Set fallback.
    defineCommand: vi.fn((name: string) => {
      redis[name] = vi.fn(async (_bloom: string, seenSet: string, frontier: string, force: string, ...pairs: string[]) => {
        let seen = memoryStore.sets.get(seenSet);
        if (!seen) {
          seen = new Set();
          memoryStore.sets.set(seenSet, seen);
        }
        const results: number[] = [];
        for (let i = 0; i < pairs.length; i += 2) {
          const isNew = !seen.has(pairs[i]);
          seen.add(pairs[i]);
          if (isNew || force === '1') {
            const list = memoryStore.lists.get(frontier) || [];
            list.unshift(pairs[i + 1]);
            memoryStore.lists.set(frontier, list);
            results.push(1);
          } else {
            results.push(0);
          }
        }
        return results;
      });
    }),
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
        get: () => {
          ops.push(() => [null, null]);
          return pipe;
        },
        zcard: () => {
          ops.push(() => [null, 0]);
          return pipe;
        },
        scard: (key: string) => {
          ops.push(() => [null, memoryStore.sets.get(key)?.size || 0]);
          return pipe;
        },
        hgetall: (key: string) => {
          ops.push(() => [null, memoryStore.hashes.get(key) || {}]);
          return pipe;
        },
        exec: async () => ops.map((op) => op()),
      };
      return pipe;
    }),
    hgetall: vi.fn(async (key: string) => memoryStore.hashes.get(key) || {}),
    quit: vi.fn(async () => 'OK'),
    on: vi.fn(),
  };
  return { redis };
});

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
    expect(body.enqueued_urls).toContain(`${uniqueUrl}/`);
    expect(body.job_ids.length).toBe(body.enqueued_count);
  });

  it('POST /api/jobs canonicalizes URLs so trivially different spellings are deduplicated', async () => {
    const host = `canon-${Date.now()}.example`;
    const first = await app.inject({ method: 'POST', url: '/api/jobs', payload: { url: `HTTPS://${host.toUpperCase()}` } });
    expect(first.statusCode).toBe(201);
    expect(JSON.parse(first.payload).url).toBe(`https://${host}/`);

    const job = JSON.parse(memoryStore.lists.get('frontier:queue')![0]);
    expect(validateCrawlTarget(job), JSON.stringify(validateCrawlTarget.errors)).toBe(true);
    expect(job.url).toBe(`https://${host}/`);
    expect(job.scope_host).toBe(host);

    const second = await app.inject({ method: 'POST', url: '/api/jobs', payload: { url: `https://${host}/#top` } });
    expect(second.statusCode).toBe(409);

    const forced = await app.inject({ method: 'POST', url: '/api/jobs', payload: { url: `https://${host}/`, force: true } });
    expect(forced.statusCode).toBe(201);
  });

  it('POST /api/jobs rejects non-HTTP schemes', async () => {
    const response = await app.inject({ method: 'POST', url: '/api/jobs', payload: { url: 'ftp://example.com/file' } });
    expect(response.statusCode).toBe(400);
  });

  it('POST /api/jobs/batch reports invalid URLs and in-batch duplicates', async () => {
    const base = `https://batch-report-${Date.now()}.example`;
    const response = await app.inject({
      method: 'POST',
      url: '/api/jobs/batch',
      payload: { urls: [base, `${base}/`, 'not a url'] },
    });

    expect(response.statusCode).toBe(201);
    const body = JSON.parse(response.payload);
    expect(body.enqueued_count).toBe(1);
    expect(body.deduplicated_count).toBe(1);
    expect(body.invalid_urls).toEqual(['not a url']);
  });

  it('parseBloomInfo reads "Number of items inserted", not "Number of filters"', () => {
    const info = ['Capacity', 1000000, 'Size', 1797880, 'Number of filters', 1, 'Number of items inserted', 42, 'Expansion rate', 2];
    expect(parseBloomInfo(info)).toBe(42);
    expect(parseBloomInfo(null)).toBe(0);
  });

  it('GET /metrics exposes Prometheus gauges and counters', async () => {
    const response = await app.inject({ method: 'GET', url: '/metrics' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.payload).toContain('# TYPE crawler_frontier_pending gauge');
    expect(response.payload).toMatch(/^crawler_documents_parsed_total \d+$/m);
  });

  it('GET /api/dead-letters validates the queue name', async () => {
    const response = await app.inject({ method: 'GET', url: '/api/dead-letters?queue=nope' });
    expect(response.statusCode).toBe(400);
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
