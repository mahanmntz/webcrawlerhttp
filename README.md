# 🕷️ Distributed Web Crawler & Scraper Monorepo

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.5-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Fastify](https://img.shields.io/badge/Fastify-4.28-black?style=for-the-badge&logo=fastify&logoColor=white)](https://fastify.dev/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

An enterprise-grade, high-throughput distributed web-to-Markdown crawler and scraper monorepo inspired by **Chapter 9 of Alex Xu's *"System Design Interview"***. Built for **AI & RAG pipelines**, it pairs high-concurrency network I/O (**Go**) with DOM parsing & clean Markdown extraction (**Python**), strict API orchestration & an interactive Web UI (**TypeScript/Fastify**), backed by **Redis**.

### 🌟 Key Differentiators (Why This Engine?)
- 🤖 **LLM-Ready Markdown Extraction**: Automatically strips boilerplate (`nav`, `header`, `footer`, `script`, ads) and outputs clean Markdown (`# Headings`, lists, code blocks, tables).
- 📉 **90%+ Token Reduction**: Live telemetry computes raw HTML vs Markdown payload savings, drastically reducing LLM inference costs and context window bloat.
- 🛡️ **Domain Boundary Guard (`stay_in_domain`)**: Constrains crawler discovery strictly to target base host/subdomains, preventing link drift into external networks (e.g. Twitter, GitHub, YouTube).
- 🖥️ **Live Cyberpunk Web Dashboard**: Instant visual console at `http://localhost:3000` with real-time cluster telemetry, one-click mission dispatch, and interactive Markdown inspector.
- 🚀 **One-Command Dev Experience**: Run `make start` to launch all microservices concurrently with zero setup overhead and graceful unified shutdown.

---

## 🏛️ System Architecture

### Component Topology
```mermaid
flowchart TD
    subgraph Ingestion ["1. URL Ingestion Layer"]
        Client["Client / CLI<br/>(make crawl-file seeds.txt)"]
        Gateway["API Gateway<br/>(TypeScript / Fastify :3000)"]
    end

    subgraph RedisBroker ["2. Distributed Broker & State (Redis 7.2)"]
        Bloom["frontier:bloom:url<br/>(Bloom Filter / Set Deduplication)"]
        FrontierQueue["frontier:queue<br/>(Pending CrawlTargets)"]
        ProcessingQueue["frontier:processing<br/>(At-Least-Once Leases)"]
        PolitenessKeys["politeness:host:domain<br/>(Distributed Rate Limits)"]
        RawPagesQueue["queue:raw_pages<br/>(Raw HTML Payloads)"]
        ContentSeen["content:seen<br/>(64-bit Body Fingerprints)"]
        ParsedDocsQueue["queue:parsed_docs<br/>(Structured Output)"]
    end

    subgraph Crawler ["3. Downloader Engine (Go)"]
        WorkerPool["Goroutine Worker Pool<br/>(Concurrent HTTP Fetching)"]
        PolitenessLimiter["Politeness Limiter<br/>(SET NX PX Leases)"]
        Transport["Optimized HTTP Transport<br/>(Keep-Alive, 5MB Max Body)"]
    end

    subgraph Web ["4. World Wide Web"]
        WebServers["Target Web Servers<br/>(news.ycombinator.com, etc.)"]
    end

    subgraph Scraper ["5. Parser & Scraper (Python)"]
        ParserWorker["Parser Worker<br/>(BeautifulSoup / lxml)"]
        Canonicalizer["URL Canonicalizer & Asset Filter"]
        Fingerprinter["64-bit SHA-256 Fingerprinter"]
    end

    Client -->|POST /api/jobs/batch| Gateway
    Gateway -->|1. Check URL Seen| Bloom
    Gateway -->|2. Push New Seeds| FrontierQueue

    FrontierQueue -->|BLMOVE + lease| ProcessingQueue
    ProcessingQueue --> WorkerPool

    WorkerPool -->|Acquire Host Lease| PolitenessKeys
    WorkerPool -->|Execute Request| Transport
    Transport -->|HTTP GET| WebServers
    WebServers -->|HTML Response| Transport

    WorkerPool -->|LPUSH RawPage| RawPagesQueue
    WorkerPool -->|LREM Acknowledge| ProcessingQueue

    RawPagesQueue -->|BLMOVE + lease| ParserWorker
    ParserWorker -->|Clean Body & Hash| Fingerprinter
    Fingerprinter -->|Check Duplicate Body| ContentSeen

    ParserWorker -->|Extract & Canonicalize| Canonicalizer
    Canonicalizer -->|Deduplicate & Re-enqueue| Bloom
    Canonicalizer -->|New Child Links| FrontierQueue

    ParserWorker -->|Store Structured Document| ParsedDocsQueue
    ParsedDocsQueue -->|GET /api/documents/export| Client
```

---

### Distributed Crawl Lifecycle
```mermaid
sequenceDiagram
    autonumber
    actor User as User / make crawl-file
    participant Gateway as API Gateway (TS)
    participant Redis as Redis Broker
    participant Go as Crawler Engine (Go)
    participant Web as Target Server
    participant Py as Parser / Scraper (Python)

    User->>Gateway: POST /api/jobs/batch (seeds.txt)
    Gateway->>Gateway: Canonicalize URLs
    Gateway->>Redis: Lua: BF.ADD (or SADD) + LPUSH frontier:queue, atomically
    alt URL is Duplicate
        Gateway-->>User: 409 Deduplicated / Batch stats
    else URL is Brand New
        Gateway-->>User: 201 Enqueued
    end

    Go->>Redis: Route: frontier:queue -> frontier:host:<host> (priority order)
    Go->>Redis: Claim: first host whose politeness window is open -> processing + lease, host locked
    Go->>Redis: GET robots:<host> (fetch robots.txt on miss)
    Go->>Web: HTTP GET (SSRF guard, Keep-Alive, 5MB cap)
    Web-->>Go: Response
    alt 2xx HTML in scope
        Go->>Redis: Settle: SET raw_page:<id>, LPUSH queue:raw_pages <id>, reopen host after max(delay, Crawl-delay)
    else Network error / 429 / 5xx
        Go->>Redis: Settle: ZADD frontier:delayed (backoff), frontier:dead after MAX_ATTEMPTS
    end

    Py->>Redis: BLMOVE queue:raw_pages -> processing + lease, GET raw_page:<id>
    Py->>Py: Parse once: Markdown, links, 64-bit SHA-256 fingerprint
    Py->>Redis: Commit (one script): ack, dedup, doc, stats, child links, job accounting
    Note over Go,Py: Reapers re-queue work whose lease expired; stale commits are refused
```

---

## ✨ Core Engineering Features

1. **Dual-Layer Deduplication (URL & Content)**:
   - **URL Layer**: Uses **RedisBloom (`BF.ADD`)** on `frontier:bloom:url` (reserved for 1M URLs at 0.1% error) for memory-efficient membership testing, with fallback to an exact Redis Set (`frontier:seen`). Checking and enqueueing a URL is one atomic Lua script, so a URL is never marked seen without being queued.
   - **Canonical URLs**: every service normalizes URLs by the same rules (scheme/host case, default ports, fragments, dot segments), verified by shared test vectors in `shared/contracts/url_canonicalization.json`.
   - **Content Layer**: Normalizes DOM bodies (stripping boilerplate scripts, headers, footers) and computes a **64-bit SHA-256 fingerprint** stored in `content:seen` to eliminate duplicate or mirror pages under different URLs.
2. **Crash-Safe Queues with Exactly-Once Effects**:
   - Both work queues use `BLMOVE`/claim into a processing list plus a lease with a deadline. A reaper re-queues work whose worker died, and dead-letters payloads that keep crashing workers.
   - Every outcome is committed by one Lua script **fenced on the lease**. If a slow worker's item was already redelivered, its commit is refused, so nothing is ever recorded twice.
   - Transient fetch failures (network errors, 429, 5xx) are retried with exponential backoff (honouring `Retry-After`) before being dead-lettered. Dead letters can be inspected and replayed through the API.
   - Fetched pages use the **claim-check** pattern: the HTML lives in `raw_page:<id>` and the queue carries only ids.
   - **Jobs complete**: each job tracks its outstanding targets exactly and moves `enqueued → running → completed`.
3. **Per-Host Back Queues (Politeness by Construction)**:
   - Following *System Design Interview* ch. 9: targets are routed into one priority-ordered queue per host, and a schedule of when each host may next be contacted decides what is claimed. There is **at most one request in flight per host**, and a gap of `max(POLITENESS_DELAY_MS, Crawl-delay, Retry-After)` after each one. Workers never spin on a busy host.
   - robots.txt is fetched once per host, cached in Redis for 24h and shared by all crawler instances; disallowed URLs are never fetched.
4. **Safe by Default**:
   - The fetcher refuses to connect to loopback, private, link-local and cloud-metadata addresses (SSRF guard, checked after DNS resolution and on every redirect). Set `ALLOW_PRIVATE_NETWORKS=true` only for local testing.
   - `stay_in_domain` crawls are bounded by the seed's host (`scope_host`), including after redirects.
   - Set `ADMIN_TOKEN` to require an `x-admin-token` header on `POST /api/cluster/reset`, and `CORS_ORIGIN` to restrict browser origins.
5. **Decoupled Polyglot Architecture**:
   - **Network I/O**: Go engine handles high-concurrency HTTP downloading with persistent connection pools (`MaxIdleConnsPerHost: 50`) and response size caps.
   - **CPU-bound Scraping**: Python service parses each DOM tree once, cleans boilerplate, and extracts metadata in an isolated process.
   - **API Gateway**: Fastify (TypeScript) performs schema validation, batch ingestion, and exposes real-time telemetry and per-job progress counters (`GET /api/jobs/:id`).
6. **Living Contract Verification**:
   - JSON Schemas in `shared/contracts/` map to Go structs, Python Pydantic models, and TypeScript interfaces. Each service's tests **validate the payloads it actually produces** against them.
   - Lua scripts shared between services live once in `shared/redis/`; each service's tests fail if its embedded copy drifts. The full Redis layout is documented in [`shared/contracts/REDIS_SPEC.md`](shared/contracts/REDIS_SPEC.md).
7. **Operable**:
   - Prometheus metrics at `GET /metrics`, dead-letter inspection and replay (`GET /api/dead-letters`, `POST /api/dead-letters/replay`), and per-job progress at `GET /api/jobs/:id`.
   - Redis runs with AOF persistence. With `OUTPUT_DIR` set, every document is also written as Markdown (one folder per job), and the Redis document list is capped by `MAX_PARSED_DOCS`.
   - CI runs every suite against Redis with and without RedisBloom, with integration tests required (not skipped), and builds all images.

---

## ⚡ Quickstart

### 🚀 One-Command Launch (Native / Fast)
Start all services (Redis check, Fastify Gateway, Go Crawler, Python Parser) with a single command:
```bash
make start
```
*(Press `Ctrl+C` at any time to cleanly stop all services together).*

---

### 🐳 Alternative: Full Docker Compose Launch
```bash
make cluster-up
```

---

## 🎯 How to Run a Crawl Test

### 1. Ingest Seed URLs
Feed URLs from `seeds.txt` directly into the distributed pipeline:
```bash
make crawl-file FILE=seeds.txt
```
*Or submit a single target:*
```bash
make submit-job URL=https://news.ycombinator.com
```

### 2. Inspect Real-Time Telemetry
Query queue depths, active in-flight crawls, and Bloom filter stats:
```bash
make get-metrics
```
*Example response:*
```json
{
  "pending_queue": 1365,
  "in_flight_processing": 13,
  "raw_pages_for_parser": 0,
  "parsed_documents_total": 83,
  "unique_urls_seen": 1475,
  "timestamp": "2026-09-28T23:02:05.055Z"
}
```

### 3. Export Extracted Documents
Download all parsed documents into `output.json`:
```bash
make export-results
```

### 4. Stop All Services
```bash
# If running via Docker Compose:
make cluster-down
```

---

## 📡 REST API & Web Console Reference

The API Gateway and Interactive Web Dashboard run at `http://localhost:3000`.

| Method | Endpoint | Description | Payload / Query | Response |
| :--- | :--- | :--- | :--- | :--- |
| `GET` | `/` | **Interactive Web UI Dashboard** | None | `200 OK` (Cyberpunk Console) |
| `POST` | `/api/jobs` | Submit a single seed URL | `{"url": "https://go.dev", "max_depth": 2, "stay_in_domain": true}` | `201 Created` or `409 Conflict` |
| `POST` | `/api/jobs/batch` | Batch ingest array of URLs | `{"urls": ["https://site1.com", "https://site2.com"], "stay_in_domain": true}` | `201 Created` with batch stats |
| `GET` | `/api/jobs/:id` | Fetch job status metadata | Path parameter `id` (UUID) | `200 OK` with job details |
| `GET` | `/api/metrics` | Real-time cluster & token metrics | None | `200 OK` (queues, Bloom, token savings %) |
| `GET` | `/api/documents` | Retrieve latest parsed documents | `?limit=20` (max 50) | `200 OK` with Markdown documents array |
| `GET` | `/api/documents/export` | Export all documents to JSON file | None | `200 OK` (`crawled_documents.json`) |
| `POST` | `/api/cluster/reset` | 🧹 Flush all Redis queues and state | None | `200 OK` |
| `GET` | `/healthz` | Cluster health probe | None | `200 OK` `{"status": "healthy"}` |

---

## 🖥️ Interactive Terminal UI (TUI) & Hotkeys

When you run `make start`, SpiderRAG launches a live, interactive terminal console powered by Python's `rich` library:

```text
╭────────────────────── SpiderRAG Engine ──────────────────────╮
│ Target: https://docs.python.org (Domain Scoped)              │
│ Progress: [████████████████░░░░] 72% | In-Flight Workers: 3  │
├──────────────────────────────────────────────────────────────┤
│ ⏳ Pending: 12   📄 Markdown Docs: 48   🛡️ Seen: 60           │
│ 📉 LLM Token Savings: -92.4% (1.4MB ➔ 110KB)                 │
│ 🧠 Total Extracted Tokens: ~24,500                          │
│ 🔗 Latest: https://docs.python.org/3/tutorial/errors.html    │
├──────────────────────────────────────────────────────────────┤
│ [L] Toggle Full Logs │ [F] Flush Queues │ [E] Export │ [Q] Quit
╰──────────────────────────────────────────────────────────────╯
```

### Interactive Hotkeys:
- **`[L]` (Toggle Logs)**: Switch between quiet telemetry mode and live scrolling logs on the fly!
- **`[F]` (Flush Queues)**: Reset and wipe all Redis queues and state to start clean.
- **`[E]` (Export JSON)**: Dump all crawled and extracted Markdown documents into a timestamped JSON file.
- **`[Q]` (Quit Cleanly)**: Broadcast graceful shutdown signals (`SIGINT`) to all microservices.

---

## 🛠️ Developer Make Targets

| Target | Description |
| :--- | :--- |
| `make start` | 🚀 **Interactive TUI**: Starts supervisor with live telemetry, hotkeys, and menu |
| `make start-raw` | Start all services with raw stdout streaming |
| `make crawl URL=https://...` | 🎯 Crawl a single website directly (cleans old queues & scopes domain) |
| `make crawl-file FILE=seeds.txt` | 📁 Batch ingest and crawl URLs from a text file |
| `make flush` / `make reset` | 🧹 Flush all Redis queues, Bloom filters, and crawler state |
| `make cluster-up` | Build and start all 4 services via Docker Compose |
| `make cluster-down` | Stop and remove cluster containers cleanly |
| `make submit-job URL=...` | Submit a single seed URL via API Gateway curl |
| `make get-metrics` | Fetch real-time cluster metrics via API Gateway |
| `make get-docs` | Fetch recent extracted documents |
| `make export-results` | Export all documents to `output.json` |
| `make test-all` | Run test suites across Go, Python, and TypeScript |

---

## 🧪 Testing Matrix

All test suites run deterministically in isolated environments without external service dependencies:
```bash
make test-all
```

- **Go**: Validates HTTP transport timeout handling and keep-alive connection pooling.
- **Python**: Validates URL canonicalization, fragment removal, asset filtering, and duplicate content detection.
- **TypeScript**: Validates Fastify request schemas, batch ingestion pipelining, and error handling.

---

## 📂 Repository Layout

```text
.
├── Makefile                     # Developer workflow and orchestration targets
├── scripts/
│   └── start.sh                 # One-command concurrent runner with unified shutdown
├── docker-compose.yml           # Multi-service composition (Redis, Go, Py, TS)
├── seeds.txt                    # Sample seed URL ingestion file
├── shared/
│   └── contracts/               # Shared JSON Schema data contracts
│       ├── crawl_target.json    # Target URL contract (Go input)
│       ├── raw_page.json        # Raw HTML contract (Go -> Python)
│       ├── parsed_document.json # Structured document contract (Python output)
│       └── REDIS_SPEC.md        # Redis key topology specification
├── services/
│   ├── api-gateway/             # Node.js / TypeScript / Fastify API service
│   │   ├── src/                 # Fastify server, routes, and Bloom filter check
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── crawler-engine/          # Go high-throughput HTTP downloader
│   │   ├── internal/            # Worker pool, Fetcher, Politeness limiter
│   │   ├── main.go
│   │   ├── Dockerfile
│   │   └── go.mod
│   └── parser-scraper/          # Python DOM extraction & link discovery
│       ├── app/                 # BeautifulSoup extractor, fingerprinting, pipeline
│       ├── parser_worker.py
│       ├── Dockerfile
│       └── requirements.txt
```

---

## 📄 License
This project is open-source under the [MIT License](LICENSE).
