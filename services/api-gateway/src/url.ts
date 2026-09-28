/**
 * Canonical URL form shared by every service that writes to the seen filter.
 * Rules and test vectors live in shared/contracts/url_canonicalization.json;
 * parser-scraper/app/urls.py implements the same rules.
 */
export function canonicalizeUrl(rawUrl: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl.trim());
  } catch {
    return null;
  }

  if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname) {
    return null;
  }

  // WHATWG URL already lowercases the host, punycodes IDNs, drops default
  // ports and resolves dot segments. The rest is up to us.
  parsed.username = '';
  parsed.password = '';
  parsed.hash = '';
  if (parsed.search === '') {
    // Clears a bare trailing '?'.
    parsed.search = '';
  }

  return parsed.toString();
}
