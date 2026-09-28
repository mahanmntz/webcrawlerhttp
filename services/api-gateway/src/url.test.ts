import { readFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { describe, it, expect } from 'vitest';
import { canonicalizeUrl } from './url.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const vectors = JSON.parse(
  readFileSync(path.join(__dirname, '../../../shared/contracts/url_canonicalization.json'), 'utf-8'),
) as { canonicalize: Array<{ input: string; expected: string | null }> };

describe('canonicalizeUrl (shared contract vectors)', () => {
  it.each(vectors.canonicalize)('$input -> $expected', ({ input, expected }) => {
    expect(canonicalizeUrl(input)).toBe(expected);
  });
});
