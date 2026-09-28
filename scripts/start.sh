#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

echo "======================================================="
echo " 🕷️  Distributed Web Crawler & Scraper Monorepo"
echo "======================================================="

# 1. Ensure Redis is up
if ! nc -z localhost 6379 >/dev/null 2>&1; then
  echo "⚡ Redis is not reachable on localhost:6379. Starting Redis container..."
  if docker ps -a | grep -q crawler-redis; then
    docker start crawler-redis >/dev/null
  else
    docker compose up -d redis >/dev/null
  fi
  sleep 1
fi

echo "✅ Redis Broker connected (localhost:6379)"

if [ "$1" = "--raw" ]; then
  echo "🚀 Starting raw stdout streaming mode..."
  trap 'echo ""; echo "🛑 Shutting down all services gracefully..."; kill $(jobs -p) 2>/dev/null || true; exit 0' SIGINT SIGTERM EXIT
  npm --prefix services/api-gateway run dev &
  PYTHONPATH=services/parser-scraper REDIS_HOST=localhost REDIS_PORT=6379 services/parser-scraper/.venv/bin/python services/parser-scraper/parser_worker.py &
  REDIS_ADDR=localhost:6379 WORKER_COUNT=3 go run ./services/crawler-engine &
  wait
else
  # Launch Interactive Terminal UI Dashboard
  exec services/parser-scraper/.venv/bin/python scripts/tui.py "$@"
fi
