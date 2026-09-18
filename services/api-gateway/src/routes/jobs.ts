import { FastifyPluginAsync } from 'fastify';
import { v4 as uuidv4 } from 'uuid';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { CrawlTarget, CreateJobRequestBody, CreateBatchJobRequestBody, BatchJobResponse } from '../types.js';

export const jobRoutes: FastifyPluginAsync = async (fastify) => {
  // Submit single seed URL
  fastify.post<{ Body: CreateJobRequestBody }>(
    '/api/jobs',
    {
      schema: {
        body: {
          type: 'object',
          required: ['url'],
          properties: {
            url: { type: 'string', format: 'uri' },
            max_depth: { type: 'integer', minimum: 0, default: 2 },
            priority: { type: 'integer', minimum: 1, maximum: 10, default: 5 },
          },
        },
      },
    },
    async (request, reply) => {
      const { url, max_depth = 2, priority = 5 } = request.body;

      try {
        const parsed = new URL(url);
        if (!['http:', 'https:'].includes(parsed.protocol)) {
          return reply.status(400).send({ error: 'Only HTTP and HTTPS URLs are supported' });
        }
      } catch {
        return reply.status(400).send({ error: 'Invalid URL format' });
      }

      const isNew = await redis.sadd(config.setSeenUrls, url);
      if (isNew === 0) {
        return reply.status(409).send({
          message: 'URL has already been submitted or crawled (Deduplicated)',
          url,
          status: 'deduplicated',
        });
      }

      const jobId = uuidv4();
      const target: CrawlTarget = {
        job_id: jobId,
        url,
        depth: 0,
        max_depth,
        priority,
        created_at: new Date().toISOString(),
      };

      const pipeline = redis.pipeline();
      pipeline.lpush(config.queueFrontier, JSON.stringify(target));
      pipeline.hset(`job:${jobId}`, {
        job_id: jobId,
        url,
        max_depth,
        priority,
        status: 'enqueued',
        created_at: target.created_at,
      });
      await pipeline.exec();

      return reply.status(201).send({
        job_id: jobId,
        url,
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
            max_depth: { type: 'integer', minimum: 0, default: 2 },
            priority: { type: 'integer', minimum: 1, maximum: 10, default: 5 },
          },
        },
      },
    },
    async (request, reply) => {
      const { urls, max_depth = 2, priority = 5 } = request.body;

      // Filter and validate URLs
      const validUrls: string[] = [];
      for (const rawUrl of urls) {
        const trimmed = rawUrl.trim();
        if (!trimmed) continue;
        try {
          const parsed = new URL(trimmed);
          if (['http:', 'https:'].includes(parsed.protocol)) {
            validUrls.push(trimmed);
          }
        } catch {
          // Skip invalid URL strings
        }
      }

      if (validUrls.length === 0) {
        return reply.status(400).send({ error: 'No valid HTTP/HTTPS URLs provided in the batch' });
      }

      // 1. Pipeline deduplication check using Redis SADD
      const checkPipeline = redis.pipeline();
      for (const url of validUrls) {
        checkPipeline.sadd(config.setSeenUrls, url);
      }
      const checkResults = await checkPipeline.exec();

      const enqueuedUrls: string[] = [];
      const deduplicatedUrls: string[] = [];
      const jobIds: string[] = [];
      const enqueuePipeline = redis.pipeline();

      const now = new Date().toISOString();

      if (checkResults) {
        checkResults.forEach(([err, result], idx) => {
          const url = validUrls[idx];
          if (!err && result === 1) {
            // New unique URL! Package as CrawlTarget
            const jobId = uuidv4();
            const target: CrawlTarget = {
              job_id: jobId,
              url,
              depth: 0,
              max_depth,
              priority,
              created_at: now,
            };

            enqueuePipeline.lpush(config.queueFrontier, JSON.stringify(target));
            enqueuePipeline.hset(`job:${jobId}`, {
              job_id: jobId,
              url,
              max_depth,
              priority,
              status: 'enqueued',
              created_at: now,
            });

            enqueuedUrls.push(url);
            jobIds.push(jobId);
          } else {
            deduplicatedUrls.push(url);
          }
        });
      }

      // Execute atomic enqueue for all valid new targets
      if (enqueuedUrls.length > 0) {
        await enqueuePipeline.exec();
      }

      const response: BatchJobResponse = {
        total_received: urls.length,
        enqueued_count: enqueuedUrls.length,
        deduplicated_count: deduplicatedUrls.length,
        enqueued_urls: enqueuedUrls,
        deduplicated_urls: deduplicatedUrls,
        job_ids: jobIds,
        message: `Batch processed: ${enqueuedUrls.length} enqueued, ${deduplicatedUrls.length} deduplicated`,
      };

      return reply.status(201).send(response);
    }
  );

  // Get Job Status
  fastify.get<{ Params: { id: string } }>('/api/jobs/:id', async (request, reply) => {
    const { id } = request.params;
    const jobData = await redis.hgetall(`job:${id}`);

    if (!jobData || Object.keys(jobData).length === 0) {
      return reply.status(404).send({ error: `Job not found: ${id}` });
    }

    return reply.send(jobData);
  });
};
