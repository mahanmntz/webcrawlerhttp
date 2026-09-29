import { IncomingHttpHeaders } from 'http';
import { timingSafeEqual } from 'crypto';
import { config } from './config.js';

/** True if ADMIN_TOKEN is unset (dev) or the x-admin-token header matches it. */
export function isAdmin(headers: IncomingHttpHeaders): boolean {
  if (!config.adminToken) return true;
  const given = Buffer.from(String(headers['x-admin-token'] ?? ''));
  const expected = Buffer.from(config.adminToken);
  return given.length === expected.length && timingSafeEqual(given, expected);
}
