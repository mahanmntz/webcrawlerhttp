.PHONY: help test-all test-go test-python test-gateway run-crawler run-parser run-gateway cluster-up cluster-down cluster-logs submit-job crawl-file get-metrics get-docs export-results seed-test seed-wikipedia check-redis read-raw-pages read-parsed-docs open-lab

help:
	@echo "Distributed Web Crawler & Scraper Monorepo"
	@echo "=========================================="
	@echo "Available commands:"
	@echo "  make cluster-up         - Build & Launch all 4 services via Docker Compose"
	@echo "  make cluster-down       - Stop all Docker containers cleanly"
	@echo "  make cluster-logs       - Follow live multi-container cluster logs"
	@echo "  make crawl-file FILE=.. - Batch ingest URLs from a line-delimited text file (default: seeds.txt)"
	@echo "  make submit-job URL=..  - Submit a single seed URL via API Gateway"
	@echo "  make get-metrics        - Fetch real-time cluster metrics via API Gateway"
	@echo "  make get-docs           - Fetch latest extracted documents via API Gateway"
	@echo "  make export-results     - Export all parsed documents to output.json"
	@echo "  make test-all           - Run all unit tests across Go, Python, and TypeScript"
	@echo "  make test-go            - Run Go unit tests"
	@echo "  make test-python        - Run Python unit tests"
	@echo "  make test-gateway       - Run TypeScript / Fastify unit tests"
	@echo "  make run-gateway        - Start API Gateway locally on port 3000"
	@echo "  make run-crawler        - Start Go Crawler Engine locally"
	@echo "  make run-parser         - Start Python Parser & Scraper locally"
	@echo "  make open-lab           - Open interactive visual lab in browser"

test-all: test-go test-python test-gateway

test-go:
	go test -v ./services/crawler-engine/...

test-python:
	PYTHONPATH=services/parser-scraper services/parser-scraper/.venv/bin/pytest -v services/parser-scraper/tests

test-gateway:
	npm --prefix services/api-gateway test

run-gateway:
	npm --prefix services/api-gateway run dev

run-crawler:
	REDIS_ADDR=localhost:6379 WORKER_COUNT=3 go run ./services/crawler-engine

run-parser:
	PYTHONPATH=services/parser-scraper REDIS_HOST=localhost REDIS_PORT=6379 services/parser-scraper/.venv/bin/python services/parser-scraper/parser_worker.py

cluster-up:
	docker compose up -d --build

cluster-down:
	docker compose down

cluster-logs:
	docker compose logs -f

submit-job:
	@curl -s -X POST http://localhost:3000/api/jobs \
	  -H "Content-Type: application/json" \
	  -d '{"url":"$(if $(URL),$(URL),https://news.ycombinator.com)","max_depth":2,"priority":5}' | jq . || \
	  curl -s -X POST http://localhost:3000/api/jobs \
	  -H "Content-Type: application/json" \
	  -d '{"url":"$(if $(URL),$(URL),https://news.ycombinator.com)","max_depth":2,"priority":5}'

crawl-file:
	@test -f $(if $(FILE),$(FILE),seeds.txt) || { echo "File not found: $(if $(FILE),$(FILE),seeds.txt)"; exit 1; }
	@node -e ' \
	  const fs = require("fs"); \
	  const file = "$(if $(FILE),$(FILE),seeds.txt)"; \
	  const lines = fs.readFileSync(file, "utf8") \
	    .split("\n") \
	    .map(l => l.trim()) \
	    .filter(l => l && !l.startsWith("#")); \
	  const payload = JSON.stringify({ urls: lines, max_depth: 2, priority: 5 }); \
	  fetch("http://localhost:3000/api/jobs/batch", { \
	    method: "POST", \
	    headers: { "Content-Type": "application/json" }, \
	    body: payload \
	  }).then(r => r.json()).then(d => console.log(JSON.stringify(d, null, 2))) \
	    .catch(e => console.error("Error connecting to API Gateway:", e.message)); \
	'

get-metrics:
	@curl -s http://localhost:3000/api/metrics | jq . || curl -s http://localhost:3000/api/metrics

get-docs:
	@curl -s http://localhost:3000/api/documents | jq . || curl -s http://localhost:3000/api/documents

export-results:
	@curl -s "http://localhost:3000/api/documents/export?format=json" > $(if $(OUT),$(OUT),output.json)
	@echo "Saved parsed documents to $(if $(OUT),$(OUT),output.json) ($$(wc -c < $(if $(OUT),$(OUT),output.json) | tr -d ' ') bytes)"

seed-test:
	docker exec crawler-redis redis-cli LPUSH frontier:queue \
	  '{"job_id":"00000000-0000-0000-0000-000000000001","url":"https://example.com","depth":0,"max_depth":2,"priority":5,"created_at":"2026-09-18T06:00:00Z"}'
	@echo "Enqueued https://example.com into frontier:queue"

seed-wikipedia:
	docker exec crawler-redis redis-cli LPUSH frontier:queue \
	  '{"job_id":"wiki-01","url":"https://en.wikipedia.org/wiki/Web_crawler","depth":0,"max_depth":2,"priority":5,"created_at":"2026-09-18T06:00:00Z"}' \
	  '{"job_id":"wiki-02","url":"https://en.wikipedia.org/wiki/Robots.txt","depth":0,"max_depth":2,"priority":5,"created_at":"2026-09-18T06:00:00Z"}' \
	  '{"job_id":"wiki-03","url":"https://en.wikipedia.org/wiki/PageRank","depth":0,"max_depth":2,"priority":5,"created_at":"2026-09-18T06:00:00Z"}'
	@echo "Enqueued 3 Wikipedia pages into frontier:queue"

check-redis:
	@echo "=== Redis Queue Status ==="
	@printf "Pending (frontier:queue): "
	@docker exec crawler-redis redis-cli LLEN frontier:queue
	@printf "In-Flight (frontier:processing): "
	@docker exec crawler-redis redis-cli LLEN frontier:processing
	@printf "Raw Pages Ready for Python (queue:raw_pages): "
	@docker exec crawler-redis redis-cli LLEN queue:raw_pages

read-raw-pages:
	@echo "=== Latest Crawled RawPage in queue:raw_pages ==="
	@docker exec crawler-redis redis-cli LINDEX queue:raw_pages 0

read-parsed-docs:
	@echo "=== Latest ParsedDocument in queue:parsed_docs ==="
	@docker exec crawler-redis redis-cli LINDEX queue:parsed_docs 0

open-lab:
	open playground/contracts-game.html
