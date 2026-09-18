export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  host: process.env.HOST || '0.0.0.0',
  redisHost: process.env.REDIS_HOST || 'localhost',
  redisPort: parseInt(process.env.REDIS_PORT || '6379', 10),
  
  // Redis Keys matching REDIS_SPEC.md
  queueFrontier: 'frontier:queue',
  queueProcessing: 'frontier:processing',
  setSeenUrls: 'frontier:seen',
  queueRawPages: 'queue:raw_pages',
  queueParsedDocs: 'queue:parsed_docs',
};
