import { FastifyPluginAsync } from 'fastify';
import { v4 as uuidv4 } from 'uuid';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { enqueueTargets } from '../frontier.js';
import { canonicalizeUrl } from '../url.js';
import { CrawlTarget, CreateJobRequestBody, CreateBatchJobRequestBody, BatchJobResponse } from '../types.js';

// Per client IP; see RATE_LIMIT_PER_MIN.
const submitRouteConfig = config.rateLimitPerMinute > 0
  ? { rateLimit: { max: config.rateLimitPerMinute, timeWindow: '1 minute' } }
  : {};

const jobOptionsSchema = {
  max_depth: { type: 'integer', minimum: 0, default: 2 },
  priority: { type: 'integer', minimum: 1, maximum: 10, default: 5 },
  stay_in_domain: { type: 'boolean', default: true },
  force: { type: 'boolean', default: false },
} as const;

function buildTarget(canonicalUrl: string, maxDepth: number, priority: number, stayInDomain: boolean, now: string): CrawlTarget {
  return {
    job_id: uuidv4(),
    url: canonicalUrl,
    depth: 0,
    max_depth: maxDepth,
    priority,
    stay_in_domain: stayInDomain,
    scope_host: new URL(canonicalUrl).hostname,
    created_at: now,
  };
}

export const jobRoutes: FastifyPluginAsync = async (fastify) => {
  // Submit single seed URL
  fastify.post<{ Body: CreateJobRequestBody }>(
    '/api/jobs',
    {
      config: submitRouteConfig,
      schema: {
        body: {
          type: 'object',
          required: ['url'],
          properties: {
            url: { type: 'string', minLength: 1 },
            ...jobOptionsSchema,
          },
        },
      },
    },
    async (request, reply) => {
      const { url, max_depth = 2, priority = 5, stay_in_domain = true, force = false } = request.body;

      const canonical = canonicalizeUrl(url);
      if (!canonical) {
        return reply.status(400).send({ error: 'Only valid HTTP and HTTPS URLs are supported' });
      }

      const target = buildTarget(canonical, max_depth, priority, stay_in_domain, new Date().toISOString());
      const [enqueued] = await enqueueTargets([target], force);
      if (!enqueued) {
        return reply.status(409).send({
          message: 'URL has already been submitted or crawled (Deduplicated)',
          url: canonical,
          status: 'deduplicated',
        });
      }

      return reply.status(201).send({
        job_id: target.job_id,
        url: canonical,
        max_depth,
        priority,
        status: 'enqueued',
        message: 'Seed URL enqueued successfully into URL Frontier',
      });
    }
  );

  // Submit batch of seed URLs
  fastify.post<{ Body: CreateBatchJobRequestBody }>(
    '/api/jobs/batch',
    {
      config: submitRouteConfig,
      schema: {
        body: {
          type: 'object',
          required: ['urls'],
          properties: {
            urls: {
              type: 'array',
              minItems: 1,
              maxItems: 1000,
              items: { type: 'string' },
            },
            ...jobOptionsSchema,
          },
        },
      },
    },
    async (request, reply) => {
      const { urls, max_depth = 2, priority = 5, stay_in_domain = true, force = false } = request.body;

      const invalidUrls: string[] = [];
      const canonicalUrls = new Set<string>();
      let duplicatesInBatch = 0;
      for (const rawUrl of urls) {
        if (!rawUrl.trim()) continue;
        const canonical = canonicalizeUrl(rawUrl);
        if (!canonical) {
          invalidUrls.push(rawUrl);
        } else if (canonicalUrls.has(canonical)) {
          duplicatesInBatch++;
        } else {
          canonicalUrls.add(canonical);
        }
      }

      if (canonicalUrls.size === 0) {
        return reply.status(400).send({ error: 'No valid HTTP/HTTPS URLs provided in the batch', invalid_urls: invalidUrls });
      }

      const now = new Date().toISOString();
      const targets = [...canonicalUrls].map((u) => buildTarget(u, max_depth, priority, stay_in_domain, now));
      const enqueuedFlags = await enqueueTargets(targets, force);

      const enqueued = targets.filter((_, i) => enqueuedFlags[i]);
      const deduplicatedUrls = targets.filter((_, i) => !enqueuedFlags[i]).map((t) => t.url);

      const response: BatchJobResponse = {
        total_received: urls.length,
        enqueued_count: enqueued.length,
        deduplicated_count: deduplicatedUrls.length + duplicatesInBatch,
        invalid_count: invalidUrls.length,
        enqueued_urls: enqueued.map((t) => t.url),
        deduplicated_urls: deduplicatedUrls,
        invalid_urls: invalidUrls,
        job_ids: enqueued.map((t) => t.job_id),
        message: `Batch processed: ${enqueued.length} enqueued, ${deduplicatedUrls.length + duplicatesInBatch} deduplicated, ${invalidUrls.length} invalid`,
      };

      return reply.status(201).send(response);
    }
  );

  // Get Job Status (includes live counters written by the crawler and parser)
  fastify.get<{ Params: { id: string } }>('/api/jobs/:id', async (request, reply) => {
    const { id } = request.params;
    const jobData = await redis.hgetall(`job:${id}`);

    if (!jobData || Object.keys(jobData).length === 0) {
      return reply.status(404).send({ error: `Job not found: ${id}` });
    }

    return reply.send(jobData);
  });
};
