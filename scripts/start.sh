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
echo "🚀 Starting API Gateway, Go Crawler, and Python Parser..."
echo "👉 Press Ctrl+C at any time to cleanly stop all services."
echo "======================================================="

# Trap to terminate all child background processes on exit
trap 'echo ""; echo "🛑 Shutting down all services gracefully..."; kill $(jobs -p) 2>/dev/null || true; exit 0' SIGINT SIGTERM EXIT

# Start API Gateway
npm --prefix services/api-gateway run dev &
PID_GATEWAY=$!

# Start Python Parser
PYTHONPATH=services/parser-scraper \
REDIS_HOST=localhost \
REDIS_PORT=6379 \
services/parser-scraper/.venv/bin/python services/parser-scraper/parser_worker.py &
PID_PARSER=$!

# Start Go Crawler Engine
REDIS_ADDR=localhost:6379 \
WORKER_COUNT=3 \
go run ./services/crawler-engine &
PID_CRAWLER=$!

# Wait for any process to exit
wait
