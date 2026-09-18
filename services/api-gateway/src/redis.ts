import Redis from 'ioredis';
import { config } from './config.js';

export const redis = new Redis({
  host: config.redisHost,
  port: config.redisPort,
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    return Math.min(times * 100, 2000);
  },
});

redis.on('connect', () => {
  console.log(`[Redis] Connected to Redis at ${config.redisHost}:${config.redisPort}`);
});

redis.on('error', (err) => {
  console.error('[Redis] Connection error:', err);
});
