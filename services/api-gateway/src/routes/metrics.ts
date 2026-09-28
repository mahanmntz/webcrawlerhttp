import { FastifyPluginAsync } from 'fastify';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { ClusterMetrics, ParsedDocument } from '../types.js';

function parseBloomInfo(value: unknown): number {
  if (!value) return 0;

  if (Array.isArray(value)) {
    for (let i = 0; i < value.length - 1; i += 2) {
      const key = String(value[i]).toLowerCase();
      const next = value[i + 1];
      if (key.includes('items') || key.includes('number')) {
        const numeric = Number(next ?? 0);
        if (!Number.isNaN(numeric)) {
          return numeric;
        }
      }
    }
    return 0;
  }

  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>);
    for (const [key, item] of entries) {
      const lowered = key.toLowerCase();
      if (lowered.includes('items') || lowered.includes('number')) {
        const numeric = Number(item ?? 0);
        if (!Number.isNaN(numeric)) {
          return numeric;
        }
      }
    }
  }

  return Number(value) || 0;
}

export const metricsRoutes: FastifyPluginAsync = async (fastify) => {
  // Real-time distributed cluster telemetry
  fastify.get('/api/metrics', async (request, reply) => {
    const pipeline = redis.pipeline();
    pipeline.llen(config.queueFrontier);
    pipeline.llen(config.queueProcessing);
    pipeline.llen(config.queueRawPages);
    pipeline.llen(config.queueParsedDocs);

    let uniqueUrlsSeen = 0;

    try {
      const bloomInfo = await redis.call('BF.INFO', config.bloomUrlSeen);
      uniqueUrlsSeen = parseBloomInfo(bloomInfo);
    } catch (err) {
      const fallback = await redis.scard(config.setSeenUrls);
      uniqueUrlsSeen = Number(fallback || 0);
    }

    const results = await pipeline.exec();
    if (!results) {
      return reply.status(500).send({ error: 'Failed to fetch cluster metrics from Redis' });
    }

    // Sample parsed documents to compute live LLM token savings
    const sampleDocsRaw = await redis.lrange(config.queueParsedDocs, 0, 49);
    let sampleRawBytes = 0;
    let sampleMdBytes = 0;
    let sampleTokens = 0;

    for (const raw of sampleDocsRaw) {
      try {
        const doc = JSON.parse(raw);
        if (doc.raw_html_bytes) sampleRawBytes += doc.raw_html_bytes;
        if (doc.markdown_bytes) sampleMdBytes += doc.markdown_bytes;
        if (doc.estimated_tokens) sampleTokens += doc.estimated_tokens;
      } catch {
        // skip unparseable
      }
    }

    const tokenSavingsPct = sampleRawBytes > 0
      ? Math.round((1 - sampleMdBytes / sampleRawBytes) * 1000) / 10
      : 0;

    const metrics: ClusterMetrics = {
      pending_queue: Number(results[0][1] || 0),
      in_flight_processing: Number(results[1][1] || 0),
      raw_pages_for_parser: Number(results[2][1] || 0),
      parsed_documents_total: Number(results[3][1] || 0),
      unique_urls_seen: uniqueUrlsSeen,
      total_markdown_tokens_est: sampleTokens,
      token_savings_pct: tokenSavingsPct,
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
