# Distributed Redis Key Topology & Message Specification

This specification documents the Redis data structures used by `api-gateway`, `crawler-engine` (Go), and `parser-scraper` (Python).

Message schemas live next to this file (`crawl_target.json`, `raw_page.json`, `parsed_document.json`). URL canonicalization and crawl-scope rules, with test vectors every service runs, live in `url_canonicalization.json`.

---

## Reliable queue pattern (used twice)

Both work queues (the URL frontier and the raw page queue) share one at-least-once layout. For a source list `<q>`:

| Key | Type | Purpose |
|---|---|---|
| `<q>` | List | Pending items. Producers `LPUSH`, consumers pop from the right. |
| `<q>:processing` | List | Items a worker has taken but not finished. |
| `<q>:leases` | Sorted set | Member = SHA-1 of the item, score = lease deadline (ms since epoch). |
| `<q>:redeliveries` | Hash | SHA-1 → times the reaper has redelivered the item. |
| `<q>:dead` | List | Dead letters: `{"payload": <item>, "reason": "...", "failed_at_ms": n}`. |

The frontier uses `frontier:queue` as `<q>`, but its other keys are named `frontier:processing`, `frontier:leases`, `frontier:redeliveries` and `frontier:dead`.

- **Take**: `BLMOVE <q> <q>:processing RIGHT LEFT <timeout>`, then `ZADD <q>:leases <now+visibility> <sha1>`.
- **Acknowledge** (`MULTI`): `LREM <q>:processing 1 <item>`, `ZREM <q>:leases <sha1>`, `HDEL <q>:redeliveries <sha1>`.
- **Dead-letter**: acknowledge + `LPUSH <q>:dead <envelope>`, in the same `MULTI`.
- **Reap** (atomic Lua, run every few seconds by every consumer instance):
  - An item in processing with no lease (its worker died between `BLMOVE` and `ZADD`) gets one.
  - An item whose lease has expired is moved back to `<q>` (`RPUSH`, so it is taken next). Once it has been redelivered more than `MAX_REDELIVERIES` times it goes to `<q>:dead` instead, so a payload that crashes workers cannot loop forever.
  - Leases whose item is gone are removed.

The reap script exists in `crawler-engine/internal/frontier/redis_frontier.go` and `parser-scraper/app/reliable_queue.py` and must stay identical.

## 1. URL Frontier (Task Queue)
- **Key**: `frontier:queue` (reliable queue, see above)
- **Payload**: `CrawlTarget` JSON
- **Producer**: `api-gateway` (seed URLs) and `parser-scraper` (discovered links), both via the atomic enqueue script (§2)
- **Consumer**: `crawler-engine`
- **Outcomes** for a dequeued target:
  - Fetched HTML (2xx) → pushed to `queue:raw_pages`, acknowledged.
  - 4xx, non-HTML, robots.txt disallow, redirect out of scope → acknowledged and counted on the job; not retried.
  - Network error, 429, 5xx → rescheduled in `frontier:delayed` with `attempts + 1` and exponential backoff (honouring `Retry-After`). After `MAX_ATTEMPTS` → `frontier:dead`.
  - Invalid URL, or a URL resolving to a non-public address (SSRF guard) → `frontier:dead`.

### Delayed targets
- **Key**: `frontier:delayed`
- **Data Type**: Sorted set. Member = `CrawlTarget` JSON, score = time it may run again (ms).
- **Used for**: retries with backoff, and targets whose host is still in its politeness window (so workers never spin on a busy host).
- **Promotion**: every crawler instance runs an atomic Lua script every 250 ms that moves due members to the head of `frontier:queue`.

## 2. Seen Filter (URL Deduplication)
- **Key**: `frontier:bloom:url`
- **Data Type**: RedisBloom `BF` filter, reserved on first use with capacity 1,000,000 and error rate 0.001 (it auto-scales past that).
- **Compatibility Key**: `frontier:seen` (`Set`), used as an exact fallback when RedisBloom is not loaded (e.g. plain `redis` images).
- **Members**: canonical URLs only (see `url_canonicalization.json`).
- **Operations**: "is it new?" and "enqueue it" happen in **one Lua script** (`ENQUEUE_TARGETS_LUA` in `api-gateway/src/frontier.ts`, and the enqueue part of `PROCESS_PAGE_LUA` in `parser-scraper/app/pipeline.py`). For each URL: `BF.ADD` (or `SADD` if the module is missing); if it returned `1`, `LPUSH frontier:queue <target>`. A URL is therefore never marked seen without being enqueued.

## 3. robots.txt Cache
- **Key Pattern**: `robots:<host[:port]>`
- **Data Type**: `String`, JSON `{"status": <http status>, "body": "<robots.txt>"}`
- **TTL**: `86400` seconds (24h); `3600` seconds for 5xx responses.
- **Semantics** (Google's): 2xx is parsed; 4xx allows everything; 5xx disallows everything. `Crawl-delay` for the crawler's product token (`ROBOTS_USER_AGENT`, default `SpiderRAG`) extends the host's politeness window, capped at `MAX_CRAWL_DELAY_SEC`.
- Crawler instances also keep policies in memory for 5 minutes.

## 4. Politeness & Rate-Limiting Leases
- **Key Pattern**: `politeness:host:<domain>` (e.g. `politeness:host:en.wikipedia.org`)
- **Data Type**: `String` with TTL
- **Purpose**: Prevent overloading hosts: at most one request per host per window.
- **Operations** (atomic Lua):
  - `SET politeness:host:<domain> <worker_id> NX PX <delay_ms>`, where `delay_ms = max(POLITENESS_DELAY_MS, Crawl-delay)`.
  - If acquired, the worker fetches.
  - Otherwise the script returns `PTTL`, and the worker parks the target in `frontier:delayed` for that long plus jitter.

## 5. Raw Page Queue (Decoupled Scraping)
- **Key**: `queue:raw_pages` (reliable queue, see above: `queue:raw_pages:processing`, `:leases`, `:redeliveries`, `:dead`)
- **Payload**: `RawPage` JSON
- **Producer**: `crawler-engine` (Go), `LPUSH`
- **Consumer**: `parser-scraper` (Python). Malformed payloads and pages that raise during parsing go straight to `queue:raw_pages:dead`. Redis errors leave the page in processing for the reaper.

## 6. Content Fingerprint Set
- **Key**: `content:seen`
- **Data Type**: `Set`
- **Purpose**: Prevent duplicate page bodies under different URLs from being re-indexed.
- **Operations**: `SADD content:seen <64-bit-body-fingerprint>` inside `PROCESS_PAGE_LUA`. Pages whose boilerplate-free text is empty are exempt, so JavaScript-rendered shells are not all treated as one duplicate.

## 7. Parsed Documents Output
- **Key**: `queue:parsed_docs`
- **Data Type**: `List`
- **Producer**: `parser-scraper` (Python), inside `PROCESS_PAGE_LUA`: the document, its content fingerprint, running totals and child links are recorded atomically. Redelivering a page after a crash is harmless: it is reported as duplicate content.
- **Consumer**: Storage/Indexing service or API Gateway

## 8. Statistics
- **Key**: `stats:totals`
- **Data Type**: `Hash` with fields `documents`, `raw_html_bytes`, `markdown_bytes`, `markdown_tokens`
- **Producer**: `parser-scraper`; read by `GET /api/metrics` and the TUI.

## 9. Jobs
- **Key Pattern**: `job:<job_id>`
- **Data Type**: `Hash`
- **Written by the gateway**: `job_id`, `url`, `max_depth`, `priority`, `stay_in_domain`, `status`, `created_at`.
- **Counters** (`HINCRBY`), shared by every page in the crawl tree:
  - Crawler: `pages_fetched`, `pages_failed`, `pages_skipped`, `fetch_retries`.
  - Parser: `pages_parsed`, `pages_duplicate`, `links_enqueued`.
