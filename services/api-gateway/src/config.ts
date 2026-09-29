export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  host: process.env.HOST || '0.0.0.0',
  redisHost: process.env.REDIS_HOST || 'localhost',
  redisPort: parseInt(process.env.REDIS_PORT || '6379', 10),

  // When set, destructive admin endpoints require the `x-admin-token` header.
  adminToken: process.env.ADMIN_TOKEN || '',
  // Max job submissions per client IP per minute (0 disables rate limiting).
  rateLimitPerMinute: parseInt(process.env.RATE_LIMIT_PER_MIN || '120', 10),
  // Comma-separated list of allowed CORS origins; unset allows any origin (dev).
  corsOrigins: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',').map((o) => o.trim()) : true,

  // Redis Keys matching REDIS_SPEC.md
  queueFrontier: 'frontier:queue',
  queueProcessing: 'frontier:processing',
  frontierHosts: 'frontier:hosts',
  frontierScheduled: 'frontier:scheduled',
  frontierLeases: 'frontier:leases',
  frontierDelayed: 'frontier:delayed',
  frontierDead: 'frontier:dead',
  frontierRedeliveries: 'frontier:redeliveries',
  setSeenUrls: 'frontier:seen',
  bloomUrlSeen: 'frontier:bloom:url',
  queueRawPages: 'queue:raw_pages',
  rawPagesProcessing: 'queue:raw_pages:processing',
  rawPagesLeases: 'queue:raw_pages:leases',
  rawPagesDead: 'queue:raw_pages:dead',
  rawPagesRedeliveries: 'queue:raw_pages:redeliveries',
  rawPagePrefix: 'raw_page:',
  queueParsedDocs: 'queue:parsed_docs',
  robotsPrefix: 'robots:',
  contentSeen: 'content:seen',
  statsTotals: 'stats:totals',
};
