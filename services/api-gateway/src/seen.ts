import { redis } from './redis.js';
import { config } from './config.js';

function canonicalizeUrl(rawUrl: string): string | null {
  try {
    const parsed = new URL(rawUrl.trim());
    if (!['http:', 'https:'].includes(parsed.protocol)) {
      return null;
    }

    parsed.hash = '';
    return parsed.toString();
  } catch {
    return null;
  }
}

export async function isNewUrl(url: string): Promise<boolean> {
  const canonical = canonicalizeUrl(url);
  if (!canonical) {
    return false;
  }

  try {
    const result = (await redis.call('BF.ADD', config.bloomUrlSeen, canonical)) as number;
    return result === 1;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);

    if (
      message.includes('ERR unknown command') ||
      message.includes('unknown command') ||
      message.includes('NOPERM')
    ) {
      const exactMatch = await redis.sadd(config.setSeenUrls, canonical);
      return exactMatch === 1;
    }

    console.error('[seen] Bloom filter check failed:', err);
    const exactMatch = await redis.sadd(config.setSeenUrls, canonical);
    return exactMatch === 1;
  }
}
