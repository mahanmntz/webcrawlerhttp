# Distributed Redis Key Topology & Message Specification

This specification documents the Redis data structures used by `api-gateway`, `crawler-engine` (Go), and `parser-scraper` (Python).

---

## 1. URL Frontier (Task Queue)
- **Key**: `frontier:queue`
- **Data Type**: `List`
- **Producer**: `api-gateway` (Seed URLs) and `parser-scraper` (Discovered links)
- **Consumer**: `crawler-engine`
- **Operations**:
  - Enqueue: `LPUSH frontier:queue <CrawlTarget_JSON>`
  - Reliable Dequeue: `RPOPLPUSH frontier:queue frontier:processing` (or `BRPOPLPUSH ... timeout`)
  - Acknowledge Completion: `LREM frontier:processing 1 <CrawlTarget_JSON>`

## 2. Seen Filter (URL Deduplication)
- **Key**: `frontier:bloom:url`
- **Data Type**: RedisBloom `BF` filter
- **Purpose**: Approximate membership testing for high-volume frontier deduplication with compact memory use.
- **Operations**:
  - `BF.ADD frontier:bloom:url <canonical_url>`
  - `BF.INFO frontier:bloom:url`
  - **Return Value**:
    - `1`: URL is new to the Bloom filter.
    - `0`: URL likely already exists; treat as deduplicated.

- **Compatibility Key**: `frontier:seen`
  - **Data Type**: `Set`
  - **Purpose**: Exact fallback / legacy set when RedisBloom is unavailable.

## 3. robots.txt Cache
- **Key Pattern**: `robots:<host>`
- **Data Type**: `String` (JSON payload)
- **TTL**: `86400` seconds (24h)
- **Purpose**: Cache robots policy per host to avoid repeated fetches and respect crawl boundaries.

## 4. Politeness & Rate-Limiting Leases
- **Key Pattern**: `politeness:host:<domain>` (e.g. `politeness:host:en.wikipedia.org`)
- **Data Type**: `String` with TTL
- **Purpose**: Prevent DDoS and respect host crawling bandwidth.
- **Operations**:
  - Acquire lease: `SET politeness:host:<domain> <worker_id> NX PX <delay_ms>`
  - If returned `OK`, the worker has permission to request the domain.
  - If returned `nil`, the domain is currently cooling down. Worker must back off or process a different domain.

## 5. Raw Page Queue (Decoupled Scraping)
- **Key**: `queue:raw_pages`
- **Data Type**: `List`
- **Producer**: `crawler-engine` (Go)
- **Consumer**: `parser-scraper` (Python)
- **Operations**:
  - Enqueue: `LPUSH queue:raw_pages <RawPage_JSON>`
  - Dequeue: `BRPOP queue:raw_pages 2` (blocking pop)

## 6. Content Fingerprint Set
- **Key**: `content:seen`
- **Data Type**: `Set`
- **Purpose**: Prevent duplicate page bodies under different URLs from being re-indexed.
- **Operations**:
  - `SADD content:seen <64-bit-body-fingerprint>`

## 7. Parsed Documents Output
- **Key**: `queue:parsed_docs`
- **Data Type**: `List`
- **Producer**: `parser-scraper` (Python)
- **Consumer**: Storage/Indexing service or API Gateway
