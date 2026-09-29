# Distributed Redis Key Topology & Message Specification

This specification documents the Redis data structures used by `api-gateway`, `crawler-engine` (Go), and `parser-scraper` (Python).

Message schemas live next to this file (`crawl_target.json`, `raw_page.json`, `parsed_document.json`), and every service's tests validate the payloads it produces against them. URL canonicalization and crawl-scope rules, with test vectors every service runs, live in `url_canonicalization.json`. Lua shared between services lives in [`shared/redis/`](../redis/); each service's tests fail if its embedded copy drifts.

---

## Delivery guarantees

- **At-least-once delivery.** Work items move into a processing list with a lease. A reaper returns items whose lease expired (their worker died).
- **Exactly-once effects.** Every outcome is committed by one Lua script that first removes the item from the processing list (`LREM … == 1`). If the reaper already handed the item to another worker, the stale worker's commit is refused and nothing changes. Job accounting (§9) relies on this.
- **Bounded redelivery.** An item redelivered more than `MAX_REDELIVERIES` times goes to a dead-letter list, so a payload that crashes workers cannot loop forever.

### Lease and reaper layout (both work queues)

| Key | Type | Purpose |
|---|---|---|
| `<processing>` | List | Items a worker holds. |
| `<leases>` | Sorted set | Member = SHA-1 of the item, score = lease deadline (ms). |
| `<redeliveries>` | Hash | SHA-1 → times the reaper redelivered the item. |
| `<dead>` | List | `{"payload": <item>, "reason": "...", "failed_at_ms": n}` |

The reaper (`shared/redis/reap.lua`) runs every few seconds on every consumer instance:
- It gives orphans (items taken without a registered lease) a lease.
- It moves expired items back to the source queue, or to `<dead>` past `MAX_REDELIVERIES`. Dead-lettering finishes the item's job.
- It removes leases whose item is gone.

## 1. URL Frontier: per-host back queues

| Key | Type | Purpose |
|---|---|---|
| `frontier:queue` | List | **Ingest.** Producers `LPUSH` CrawlTarget JSON. |
| `frontier:host:<host>` | Sorted set | Targets for one host. Score = `(10 - priority) * 1e13 + enqueued_ms`: higher priority first, then FIFO. |
| `frontier:hosts` | Sorted set | Host → time (ms) it may next be contacted. |
| `frontier:scheduled` | String (counter) | Number of targets in all host queues. |
| `frontier:processing`, `frontier:leases`, `frontier:redeliveries`, `frontier:dead` | | Lease layout above. |
| `frontier:delayed` | Sorted set | Retries: member = CrawlTarget JSON, score = time it may run again. |

Flow (all steps are atomic Lua in `crawler-engine/internal/frontier`):
1. **Route**: every crawler instance runs this every 100 ms. It `RPOP`s ingest, extracts the host (`host[:port]`, lowercased) and `ZADD`s the target to `frontier:host:<host>`. If the host is new it is added to `frontier:hosts` as ready now. Targets with no http(s) host go to `frontier:dead`.
2. **Claim**: a worker takes the first host in `frontier:hosts` whose time ≤ now, `ZPOPMIN`s its best target into `frontier:processing` (with a lease), and locks the host (score = now + visibility timeout). Hosts found empty after their window are dropped. There is **at most one request in flight per host**, and workers never spin: when nothing is ready they sleep until the earliest host frees up (capped at 250 ms).
3. **Settle** (fenced). This also reopens the host at `now + gap`, where `gap = max(POLITENESS_DELAY_MS, robots Crawl-delay, Retry-After)`; the gap is 0 when no request was made (robots disallow, invalid URL). Outcomes:
   - **push**: a 2xx HTML page goes to the parser (§5).
   - **finish**: 4xx, non-HTML, robots.txt disallow, redirect out of scope. Finishes the job's item.
   - **delay**: network error, 429 or 5xx. The target is rescheduled in `frontier:delayed` with `attempts + 1` and exponential backoff (honouring `Retry-After`).
   - **dead**: attempts exhausted, invalid URL, SSRF-blocked address, or corrupt payload. Goes to `frontier:dead` and finishes the job's item.
   - **release**: shutdown mid-fetch. Straight back to ingest.
4. **Promote**: every 250 ms, due members of `frontier:delayed` go back to ingest.

## 2. Seen Filter (URL Deduplication)
- **Key**: `frontier:bloom:url`: a RedisBloom filter, reserved on first use with capacity 1,000,000 and error rate 0.001 (auto-scaling past that). About 0.1% of genuinely new URLs are treated as already seen, the usual Bloom-filter trade-off.
- **Fallback Key**: `frontier:seen` (`Set`), an exact fallback when RedisBloom is not loaded (plain `redis` images).
- **Members**: canonical URLs only.
- **Operations**: `shared/redis/enqueue_targets.lua` marks a URL seen and enqueues it in one step. Used by the gateway (for seeds, it also creates the job hash) and by the parser (for discovered links).

URLs are remembered for the lifetime of the filter. Re-crawling a URL requires `force: true`, or clearing the filter.

## 3. robots.txt Cache
- **Key Pattern**: `robots:<host[:port]>`
- **Data Type**: `String`, JSON `{"status": <http status>, "body": "<robots.txt>"}`
- **TTL**: `86400` seconds (24h); `3600` seconds for 5xx responses.
- **Semantics** (Google's):
  - 2xx is parsed; 4xx allows everything; 5xx disallows everything.
  - `Crawl-delay` for `ROBOTS_USER_AGENT` (default `SpiderRAG`) widens the host's gap, capped at `MAX_CRAWL_DELAY_SEC`.
- Crawler instances also keep policies in memory for 5 minutes.

## 4. Politeness
Enforced structurally by the host schedule in §1: one in-flight request per host, and a gap after each request. There are no separate lease keys.

## 5. Raw Page Queue (claim-check)
| Key | Type | Purpose |
|---|---|---|
| `raw_page:<id>` | String, TTL 7 days | RawPage JSON. `<id>` is `<job_id>/<random hex>`. |
| `queue:raw_pages` | List | Page ids. |
| `queue:raw_pages:processing`, `:leases`, `:redeliveries`, `:dead` | | Lease layout above, over ids. |

- The crawler's push settle writes `raw_page:<id>` and `LPUSH`es the id in the same script.
- The parser `BLMOVE`s ids into processing and reads the payload. Malformed or expired payloads, and pages that raise while parsing, are dead-lettered (fenced, and they finish the job).
- Queue operations and the reaper never copy page bodies.

## 6. Content Fingerprint Set
- **Key**: `content:seen` (`Set`) of 64-bit SHA-256 fingerprints of normalized `<body>` text (boilerplate removed).
- Pages with no text are exempt, so JavaScript-rendered shells are not all treated as duplicates of each other.

## 7. Parsed Documents
- **Key**: `queue:parsed_docs` (`List`). Holds the newest `MAX_PARSED_DOCS` documents (default 10,000; `LTRIM`).
- **Durable copy**: with `OUTPUT_DIR` set, every document is also written to `OUTPUT_DIR/<job_id>/<sha1(url)[:16]>.md` with YAML front matter.
  - The file is written before the commit, so a crash leads to a rewrite on redelivery, never a missing file.
  - The file is removed if the commit finds the content to be a duplicate.
- **Commit**: `PROCESS_PAGE_LUA` in `parser-scraper/app/pipeline.py`. It covers the fence and ack, deleting `raw_page:<id>`, content dedup, the document, `LTRIM`, totals, child targets, and job accounting.

## 8. Statistics
- **Key**: `stats:totals`, a `Hash` with fields `documents`, `raw_html_bytes`, `markdown_bytes`, `markdown_tokens`. These are all-time totals, unaffected by `LTRIM`.
- Exposed by `GET /api/metrics` and in Prometheus format by `GET /metrics`.

## 9. Jobs
- **Key Pattern**: `job:<job_id>` (`Hash`)
- **Seed fields** (written by the gateway's enqueue script): `job_id`, `url`, `max_depth`, `priority`, `stay_in_domain`, `created_at`.
- **Lifecycle**: `status` goes `enqueued` → `running` (first claim) → `completed` (with `completed_at_ms`). `outstanding` counts targets enqueued but not yet finished:
  - `+1` when a target is enqueued (seed or discovered link), and when a dead letter is replayed.
  - `-1` when a target finishes: crawler finish/dead outcomes, a page parsed or found duplicate, dead-lettered by the reaper, the router or the parser.
  - The parser adds a page's children before finishing the page, so a job cannot complete early. Fenced settles make every decrement happen exactly once.
- **Progress counters** (`HINCRBY`):
  - Crawler: `pages_fetched`, `pages_failed`, `pages_skipped`, `fetch_retries`.
  - Parser: `pages_parsed`, `pages_duplicate`, `links_enqueued`.

## 10. Operations API (gateway)
- `GET /api/dead-letters?queue=frontier|raw_pages&limit=n` returns the newest dead letters.
- `POST /api/dead-letters/replay {"queue", "count"}` requeues the oldest dead letters: targets with `attempts` reset, raw pages only while their payload exists. Replayed jobs reopen. Requires `x-admin-token` when `ADMIN_TOKEN` is set.
- `GET /metrics` serves Prometheus metrics.
- `POST /api/cluster/reset` flushes all crawler state (admin).
