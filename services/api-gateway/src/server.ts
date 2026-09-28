import path from 'path';
import { fileURLToPath } from 'url';
import Fastify from 'fastify';
import fastifyCors from '@fastify/cors';
import fastifyStatic from '@fastify/static';
import { config } from './config.js';
import { redis } from './redis.js';
import { jobRoutes } from './routes/jobs.js';
import { metricsRoutes } from './routes/metrics.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export function buildServer() {
  const app = Fastify({
    logger: false,
  });

  // Enable CORS
  app.register(fastifyCors, {
    origin: true,
  });

  // Serve modern Web UI Dashboard
  app.register(fastifyStatic, {
    root: path.join(__dirname, '../public'),
    prefix: '/',
  });

  // Clean structured request logger
  app.addHook('onResponse', (request, reply, done) => {
    if (request.url !== '/healthz' && !request.url.startsWith('/public') && !request.url.includes('.')) {
      const icon = reply.statusCode < 400 ? '✅' : '⚠️';
      console.log(`[GATEWAY] ${icon} ${request.method} ${request.url} -> HTTP ${reply.statusCode} (${Math.round(reply.elapsedTime)}ms)`);
    }
    done();
  });

  // Healthcheck endpoint
  app.get('/healthz', async () => {
    return { status: 'healthy', timestamp: new Date().toISOString() };
  });

  // Register Routes
  app.register(jobRoutes);
  app.register(metricsRoutes);

  return app;
}

async function start() {
  const app = buildServer();

  // Handle OS signals for graceful shutdown
  const signals = ['SIGINT', 'SIGTERM'] as const;
  for (const signal of signals) {
    process.on(signal, async () => {
      console.log(`\n[API Gateway] Received ${signal}. Closing server gracefully...`);
      await app.close();
      await redis.quit();
      console.log('[API Gateway] Shutdown complete. Goodbye!');
      process.exit(0);
    });
  }

  try {
    await app.listen({ port: config.port, host: config.host });
    console.log(`🚀 API Gateway running at http://${config.host}:${config.port}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
}

// Only start when invoked directly
if (process.env.NODE_ENV !== 'test') {
  start();
}
