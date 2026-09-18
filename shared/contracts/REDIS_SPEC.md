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
- **Key**: `frontier:seen`
- **Data Type**: `Set`
- **Purpose**: Fast $O(1)$ lookup to guarantee a URL is never crawled or enqueued more than once.
- **Operations**:
  - `SADD frontier:seen <canonical_url>`
  - **Return Value**:
    - `1`: URL is brand new. Safe to enqueue into `frontier:queue`.
    - `0`: URL has already been processed or is currently in flight. Skip!

## 3. Politeness & Rate-Limiting Leases
- **Key Pattern**: `politeness:host:<domain>` (e.g. `politeness:host:en.wikipedia.org`)
- **Data Type**: `String` with TTL
- **Purpose**: Prevent DDoS and respect host crawling bandwidth.
- **Operations**:
  - Acquire lease: `SET politeness:host:<domain> <worker_id> NX PX <delay_ms>`
  - If returned `OK`, the worker has permission to request the domain.
  - If returned `nil`, the domain is currently cooling down. Worker must back off or process a different domain.

## 4. Raw Page Queue (Decoupled Scraping)
- **Key**: `queue:raw_pages`
- **Data Type**: `List`
- **Producer**: `crawler-engine` (Go)
- **Consumer**: `parser-scraper` (Python)
- **Operations**:
  - Enqueue: `LPUSH queue:raw_pages <RawPage_JSON>`
  - Dequeue: `BRPOP queue:raw_pages 2` (blocking pop)

## 5. Parsed Documents Output
- **Key**: `queue:parsed_docs`
- **Data Type**: `List`
- **Producer**: `parser-scraper` (Python)
- **Consumer**: Storage/Indexing service or API Gateway
