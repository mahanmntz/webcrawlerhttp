import { readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { describe, it, expect } from 'vitest';
import { ENQUEUE_TARGETS_FN } from './frontier.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

describe('shared Lua', () => {
  it('ENQUEUE_TARGETS_FN matches shared/redis/enqueue_targets.lua', () => {
    const canonical = readFileSync(path.join(__dirname, '../../../shared/redis/enqueue_targets.lua'), 'utf-8');
    expect(ENQUEUE_TARGETS_FN.trim()).toBe(canonical.trim());
  });
});
