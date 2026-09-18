import { describe, it, expect, afterAll } from 'vitest';
import { buildServer } from './server.js';
import { redis } from './redis.js';

describe('API Gateway Server Tests', () => {
  const app = buildServer();

  afterAll(async () => {
    await app.close();
    await redis.quit();
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
});
