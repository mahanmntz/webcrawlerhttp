# 🕷️ Distributed Web Crawler & Scraper Monorepo

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.5-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Fastify](https://img.shields.io/badge/Fastify-4.28-black?style=for-the-badge&logo=fastify&logoColor=white)](https://fastify.dev/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

An enterprise-grade, high-throughput distributed web crawler and scraper monorepo inspired by **Chapter 9 of Alex Xu's *"System Design Interview"*** (URL Frontier, URL Seen Deduplication, Politeness/Rate-Limiting, Decoupled Scraping).

---

## 🏛️ System Architecture

```text
                           +----------------------------+
                           |  Client / Ingestion Script |
                           +--------------+-------------+
                                          |
                POST /api/jobs            |  POST /api/jobs/batch (seeds.txt)
                                          v
                           +----------------------------+
                           |   API Gateway (Fastify)    |
                           |  - TypeScript / Node.js    |
                           |  - Strict Schema Validation|
                           |  - Deduplication Check     |
                           +--------------+-------------+
                                          |
                                          v  LPUSH (CrawlTarget JSON)
  +-----------------------------------------------------------------------------------+
  |                             Distributed State (Redis 7.2)                         |
  |  - `frontier:queue`       : List of pending CrawlTargets                          |
  |  - `frontier:processing`  : In-flight targets (At-least-once reliable queue)      |
  |  - `frontier:seen`        : Set for O(1) URL deduplication                        |
  |  - `politeness:host:<d>`  : Per-domain temporary leases with TTL for rate limits  |
  |  - `queue:raw_pages`      : Downloaded raw HTML payloads                          |
  |  - `queue:parsed_docs`    : Normalized structured documents                       |
  +-----------------------+-----------------------------------+-----------------------+
                          |                                   ^
                          v  BRPOPLPUSH                       | LPUSH (Discovered URLs)
  +---------------------------------------+                   |
  |      Crawler Engine (Go)              |                   |
  |  - Fixed Goroutine Worker Pool        |                   |
  |  - Connection Pooling & Keep-Alive    |                   |
  |  - Host Politeness Limiter (SET NX PX)|                   |
  |  - Graceful Shutdown (SIGINT/SIGTERM) |                   |
  +-----------------------+---------------+                   |
                          |                                   |
                          v  LPUSH (RawPage JSON)             |
  +---------------------------------------+                   |
  |      Parser & Scraper (Python)        |                   |
  |  - BeautifulSoup / lxml DOM Parsing   |                   |
  |  - URL Canonicalization & Fragment Del+-------------------+
  |  - Media/Script Asset Filtering       |
  |  - Pushes structured doc to output    |
  +---------------------------------------+
```

---

## ✨ Core Engineering Features

1. **Distributed URL Frontier**:
   - Reliable queue semantics via `BRPOPLPUSH` into `frontier:processing` to guarantee **at-least-once delivery** even if a worker crashes mid-crawl.
2. **Atomic URL Deduplication**:
   - Fast $O(1)$ duplicate filtering via Redis Sets (`SADD frontier:seen`). Duplicate submissions return immediate HTTP `409 Conflict`.
3. **Per-Host Politeness & Rate-Limiting**:
   - Enforces a delay per domain using distributed Redis leases (`SET politeness:host:<domain> <worker_id> NX PX 1000`) to prevent overwhelming target servers.
4. **Decoupled Architecture (I/O vs CPU)**:
   - Separates network I/O-bound HTML downloading (Go) from CPU-bound DOM parsing and text extraction (Python) using asynchronous Redis queues.
5. **Production HTTP Client Tuning**:
   - Custom Go `http.Transport` with Keep-Alive, TCP connection reuse (`MaxIdleConnsPerHost: 50`), socket timeouts, and a 5MB payload limit to prevent memory exhaustion bombs.
6. **URL Canonicalization Engine**:
   - Resolves relative paths (`/about` $\to$ absolute), strips URL fragments (`#top`), and filters out media/script extensions (`.png`, `.pdf`, `.zip`, `.js`).

---

## ⚡ Quickstart in 60 Seconds

### Prerequisites
- [Docker](https://www.docker.com/) & Docker Compose
- (Optional for local development) [Go 1.22+](https://go.dev/), [Node.js 20+](https://nodejs.org/), [Python 3.11+](https://www.python.org/)

### 1. Launch the Cluster
Build and start all 4 containers (Redis, API Gateway, Go Engine, Python Parser) in the background:
```bash
make cluster-up
```

### 2. Batch Ingest Seed URLs
Feed URLs from the sample `seeds.txt` file into the distributed pipeline:
```bash
make crawl-file FILE=seeds.txt
```
*Or submit a single target:*
```bash
make submit-job URL=https://news.ycombinator.com
```

### 3. Monitor Real-Time Cluster Metrics
Query queue lengths, active workers, and throughput:
```bash
make get-metrics
```
*Example response:*
```json
{
  "pending_queue": 1422,
  "in_flight_processing": 10,
  "raw_pages_for_parser": 0,
  "parsed_documents_total": 31,
  "unique_urls_seen": 1464,
  "timestamp": "2026-09-18T03:18:01.116Z"
}
```

### 4. Export Extracted Data to File
Download all parsed structured documents into `output.json`:
```bash
make export-results
```

### 5. Follow Live Cluster Logs
```bash
make cluster-logs
```

### 6. Stop the Cluster
```bash
make cluster-down
```

---

## 📡 REST API Reference

The API Gateway runs on port `3000` (`http://localhost:3000`).

| Method | Endpoint | Description | Request Body / Query | Success Response |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/api/jobs` | Submit a single seed URL | `{"url": "https://go.dev", "max_depth": 2}` | `201 Created` or `409 Deduplicated` |
| `POST` | `/api/jobs/batch` | Batch ingest array of URLs | `{"urls": ["https://site1.com", "https://site2.com"]}` | `201 Created` with batch stats |
| `GET` | `/api/jobs/:id` | Fetch metadata of a crawl job | Path parameter `id` (UUID) | `200 OK` with job details |
| `GET` | `/api/metrics` | Real-time cluster telemetry | None | `200 OK` (queue lengths & seen count) |
| `GET` | `/api/documents` | Retrieve latest parsed documents | `?limit=20` (max 50) | `200 OK` with documents array |
| `GET` | `/api/documents/export` | Export all documents to file | `?format=json` | `200 OK` (attachment `output.json`) |
| `GET` | `/healthz` | Cluster liveness probe | None | `200 OK` `{"status": "healthy"}` |

---

## 🧪 Testing Matrix

Run the automated test suites across all 3 programming languages in under 1 second:
```bash
make test-all
```

To run individual language suites:
```bash
make test-go        # Go unit tests (httptest & connection pool)
make test-python    # Python Pytest (extractor & canonicalizer)
make test-gateway   # Vitest (Fastify route validation & schemas)
```

---

## 📂 Repository Structure

```text
.
├── Makefile                     # Root developer orchestration commands
├── docker-compose.yml           # Multi-container orchestration (Redis, Go, Py, TS)
├── seeds.txt                    # Sample seed URL ingestion file
├── OVERVIEW.md                  # Comprehensive architectural deep-dive (Persian)
├── shared/
│   └── contracts/               # Shared JSON Schema data contracts
│       ├── crawl_target.json    # Target URL contract (Go input)
│       ├── raw_page.json        # Raw HTML contract (Go -> Python)
│       ├── parsed_document.json # Structured document contract (Python output)
│       └── REDIS_SPEC.md        # Redis key topology specification
├── services/
│   ├── api-gateway/             # Node.js + TypeScript + Fastify
│   │   ├── src/                 # Fastify server, routes, and Redis client
│   │   ├── Dockerfile
│   │   └── package.json
│   ├── crawler-engine/          # Go high-performance downloader
│   │   ├── internal/            # Worker pool, Fetcher, Politeness limiter
│   │   ├── main.go
│   │   ├── Dockerfile
│   │   └── go.mod
│   └── parser-scraper/          # Python DOM extraction & link discovery
│       ├── app/                 # BeautifulSoup extractor & pipeline
│       ├── parser_worker.py
│       ├── Dockerfile
│       └── requirements.txt
└── playground/
    └── contracts-game.html      # Interactive visual lab & sandbox dashboard
```

---

## 🎮 Interactive Visual Lab

An interactive browser-based learning sandbox is included to explore concurrency behaviors, worker pools, crash simulations, and link canonicalization visually.

Launch it with:
```bash
make open-lab
```

---

## 📄 License
This project is open-source under the [MIT License](LICENSE).
