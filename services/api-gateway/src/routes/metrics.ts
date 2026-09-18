import { FastifyPluginAsync } from 'fastify';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { ClusterMetrics, ParsedDocument } from '../types.js';

export const metricsRoutes: FastifyPluginAsync = async (fastify) => {
  // Real-time distributed cluster telemetry
  fastify.get('/api/metrics', async (request, reply) => {
    const pipeline = redis.pipeline();
    pipeline.llen(config.queueFrontier);
    pipeline.llen(config.queueProcessing);
    pipeline.llen(config.queueRawPages);
    pipeline.llen(config.queueParsedDocs);
    pipeline.scard(config.setSeenUrls);

    const results = await pipeline.exec();
    if (!results) {
      return reply.status(500).send({ error: 'Failed to fetch cluster metrics from Redis' });
    }

    const metrics: ClusterMetrics = {
      pending_queue: Number(results[0][1] || 0),
      in_flight_processing: Number(results[1][1] || 0),
      raw_pages_for_parser: Number(results[2][1] || 0),
      parsed_documents_total: Number(results[3][1] || 0),
      unique_urls_seen: Number(results[4][1] || 0),
      timestamp: new Date().toISOString(),
    };

    return reply.send(metrics);
  });

  // Query extracted structured documents (paginated/limited)
  fastify.get<{ Querystring: { limit?: number } }>('/api/documents', async (request, reply) => {
    const limit = Math.min(Number(request.query.limit || 10), 50);
    const rawDocs = await redis.lrange(config.queueParsedDocs, 0, limit - 1);

    const documents: ParsedDocument[] = rawDocs
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    return reply.send({
      count: documents.length,
      documents,
    });
  });

  // Export all extracted documents as downloadable or readable JSON
  fastify.get<{ Querystring: { format?: string } }>('/api/documents/export', async (request, reply) => {
    const rawDocs = await redis.lrange(config.queueParsedDocs, 0, -1);

    const documents: ParsedDocument[] = rawDocs
      .map((item) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    reply.header('Content-Type', 'application/json; charset=utf-8');
    reply.header('Content-Disposition', 'attachment; filename="crawled_documents.json"');

    return reply.send({
      exported_at: new Date().toISOString(),
      total_count: documents.length,
      documents,
    });
  });
};
