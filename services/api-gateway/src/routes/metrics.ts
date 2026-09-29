import { FastifyPluginAsync } from 'fastify';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { ClusterMetrics, ParsedDocument } from '../types.js';
import { isAdmin } from '../auth.js';

// BF.INFO replies with pairs like ["Capacity", n, "Size", n, "Number of filters", n,
// "Number of items inserted", n, ...]. Only the "items inserted" count is wanted.
export function parseBloomInfo(value: unknown): number {
  const pairs: Array<[string, unknown]> = [];
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length - 1; i += 2) pairs.push([String(value[i]), value[i + 1]]);
  } else if (value && typeof value === 'object') {
    pairs.push(...Object.entries(value as Record<string, unknown>));
  }

  for (const [key, item] of pairs) {
    if (key.toLowerCase().includes('inserted')) {
      const numeric = Number(item ?? 0);
      return Number.isNaN(numeric) ? 0 : numeric;
    }
  }
  return 0;
}

async function deleteByPattern(pattern: string): Promise<number> {
  let cursor = '0';
  let deleted = 0;
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 500);
    cursor = next;
    if (keys.length > 0) deleted += await redis.unlink(...keys);
  } while (cursor !== '0');
  return deleted;
}

export async function collectMetrics(): Promise<ClusterMetrics> {
  const pipeline = redis.pipeline();
  pipeline.llen(config.queueFrontier);
  pipeline.get(config.frontierScheduled);
  pipeline.zcard(config.frontierHosts);
  pipeline.llen(config.queueProcessing);
  pipeline.zcard(config.frontierDelayed);
  pipeline.llen(config.frontierDead);
  pipeline.llen(config.queueRawPages);
  pipeline.llen(config.rawPagesProcessing);
  pipeline.llen(config.rawPagesDead);
  pipeline.llen(config.queueParsedDocs);
  pipeline.hgetall(config.statsTotals);
  pipeline.scard(config.setSeenUrls);

  const results = await pipeline.exec();
  if (!results) {
    throw new Error('Failed to fetch cluster metrics from Redis');
  }
  const n = (i: number) => Number(results[i][1] || 0);

  // RedisBloom is optional; the exact Set is used when it is not loaded.
  let uniqueUrlsSeen = n(11);
  try {
    uniqueUrlsSeen += parseBloomInfo(await redis.call('BF.INFO', config.bloomUrlSeen));
  } catch {
    // No Bloom filter (module missing or key not created yet).
  }

  // Running totals maintained by the parser for every parsed document.
  const totals = (results[10][1] || {}) as Record<string, string>;
  const rawBytes = Number(totals.raw_html_bytes || 0);
  const mdBytes = Number(totals.markdown_bytes || 0);

  return {
    pending_queue: n(0) + n(1),
    ingest_queue: n(0),
    scheduled_in_host_queues: n(1),
    active_hosts: n(2),
    in_flight_processing: n(3),
    delayed_retry: n(4),
    dead_letter: n(5),
    raw_pages_for_parser: n(6),
    raw_pages_in_flight: n(7),
    raw_pages_dead_letter: n(8),
    parsed_documents_total: Number(totals.documents || 0),
    parsed_documents_retained: n(9),
    unique_urls_seen: uniqueUrlsSeen,
    total_markdown_tokens_est: Number(totals.markdown_tokens || 0),
    token_savings_pct: rawBytes > 0 ? Math.round((1 - mdBytes / rawBytes) * 1000) / 10 : 0,
    timestamp: new Date().toISOString(),
  };
}

export const metricsRoutes: FastifyPluginAsync = async (fastify) => {
  // Real-time distributed cluster telemetry
  fastify.get('/api/metrics', async (request, reply) => {
    return reply.send(await collectMetrics());
  });

  // Prometheus exposition format, for scraping and alerting.
  fastify.get('/metrics', async (request, reply) => {
    const m = await collectMetrics();
    const gauges: Array<[string, string, number]> = [
      ['crawler_frontier_pending', 'Targets waiting to be crawled (ingest + host queues)', m.pending_queue],
      ['crawler_frontier_active_hosts', 'Hosts with scheduled targets or a politeness window', m.active_hosts],
      ['crawler_frontier_in_flight', 'Targets claimed by a crawler worker', m.in_flight_processing],
      ['crawler_frontier_delayed', 'Targets waiting for a retry', m.delayed_retry],
      ['crawler_frontier_dead_letters', 'Targets in the frontier dead-letter list', m.dead_letter],
      ['crawler_raw_pages_pending', 'Fetched pages waiting for the parser', m.raw_pages_for_parser],
      ['crawler_raw_pages_in_flight', 'Pages being parsed', m.raw_pages_in_flight],
      ['crawler_raw_pages_dead_letters', 'Pages in the parser dead-letter list', m.raw_pages_dead_letter],
      ['crawler_urls_seen', 'Unique URLs recorded by the seen filter', m.unique_urls_seen],
    ];
    const counters: Array<[string, string, number]> = [
      ['crawler_documents_parsed_total', 'Documents parsed', m.parsed_documents_total],
      ['crawler_markdown_tokens_total', 'Estimated Markdown tokens produced', m.total_markdown_tokens_est ?? 0],
    ];

    const lines: string[] = [];
    for (const [type, metrics] of [['gauge', gauges], ['counter', counters]] as const) {
      for (const [name, help, value] of metrics) {
        lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`, `${name} ${value}`);
      }
    }
    reply.header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    return reply.send(lines.join('\n') + '\n');
  });

  // Query extracted structured documents (paginated/limited)
  fastify.get<{ Querystring: { limit?: number } }>('/api/documents', async (request, reply) => {
    const limit = Math.min(Number(request.query.limit || 10), 50);
    const rawDocs = await redis.lrange(config.queueParsedDocs, 0, limit - 1);

    const documents: ParsedDocument[] = rawDocs
      .map((item: string) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter((doc): doc is ParsedDocument => doc !== null);

    return reply.send({
      count: documents.length,
      documents,
    });
  });

  // Export all extracted documents as downloadable or readable JSON
  fastify.get<{ Querystring: { format?: string } }>('/api/documents/export', async (request, reply) => {
    const rawDocs = await redis.lrange(config.queueParsedDocs, 0, -1);

    const documents: ParsedDocument[] = rawDocs
      .map((item: string) => {
        try {
          return JSON.parse(item);
        } catch {
          return null;
        }
      })
      .filter((doc): doc is ParsedDocument => doc !== null);

    reply.header('Content-Type', 'application/json; charset=utf-8');
    reply.header('Content-Disposition', 'attachment; filename="crawled_documents.json"');

    return reply.send({
      exported_at: new Date().toISOString(),
      total_count: documents.length,
      documents,
    });
  });

  // Reset / Flush all crawler queues and state
  fastify.post('/api/cluster/reset', async (request, reply) => {
    if (!isAdmin(request.headers)) {
      return reply.status(401).send({ error: 'Missing or invalid x-admin-token header' });
    }

    try {
      await redis.unlink(
        config.queueFrontier,
        config.queueProcessing,
        config.frontierHosts,
        config.frontierScheduled,
        config.frontierLeases,
        config.frontierDelayed,
        config.frontierDead,
        config.frontierRedeliveries,
        config.setSeenUrls,
        config.bloomUrlSeen,
        config.queueRawPages,
        config.rawPagesProcessing,
        config.rawPagesLeases,
        config.rawPagesDead,
        config.rawPagesRedeliveries,
        config.queueParsedDocs,
        config.contentSeen,
        config.statsTotals,
      );

      // SCAN instead of KEYS so a large keyspace doesn't block Redis.
      await deleteByPattern('job:*');
      await deleteByPattern('frontier:host:*');
      await deleteByPattern(`${config.rawPagePrefix}*`);

      return reply.send({
        status: 'reset_successful',
        message: 'All queues, seen sets, and crawler state flushed cleanly',
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error('[cluster.reset] Error flushing Redis state:', err);
      return reply.status(500).send({ error: 'Failed to flush cluster state' });
    }
  });
};
