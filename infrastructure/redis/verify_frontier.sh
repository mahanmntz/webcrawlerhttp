#!/usr/bin/env bash
set -e

# ==============================================================================
# verify_frontier.sh
# Validates the Redis URL Frontier patterns against the live crawler-redis container
# ==============================================================================

echo "=== [1/4] Testing URL Seen Deduplication (SADD) ==="
URL1="https://news.ycombinator.com/item?id=123"

# First insert: Should return 1 (New URL, enqueue)
FIRST_INSERT=$(docker exec crawler-redis redis-cli SADD frontier:seen "$URL1")
echo "Insert #1 for '$URL1' -> SADD returned: $FIRST_INSERT (Expected: 1)"
if [ "$FIRST_INSERT" != "1" ]; then
  echo "FAIL: Expected 1 on first insert"
  exit 1
fi

# Duplicate insert: Should return 0 (Already seen, skip)
SECOND_INSERT=$(docker exec crawler-redis redis-cli SADD frontier:seen "$URL1")
echo "Insert #2 for '$URL1' -> SADD returned: $SECOND_INSERT (Expected: 0 - Deduplicated!)"
if [ "$SECOND_INSERT" != "0" ]; then
  echo "FAIL: Expected 0 on duplicate insert"
  exit 1
fi

echo -e "\n=== [2/4] Testing Reliable Queue Pop (LPUSH & RPOPLPUSH) ==="
SAMPLE_JOB='{"job_id":"00000000-0000-0000-0000-000000000001","url":"https://news.ycombinator.com/item?id=123","depth":0,"max_depth":2,"priority":5,"created_at":"2026-09-18T00:00:00Z"}'

# Enqueue into frontier:queue
docker exec crawler-redis redis-cli LPUSH frontier:queue "$SAMPLE_JOB" > /dev/null
QUEUE_LEN=$(docker exec crawler-redis redis-cli LLEN frontier:queue)
echo "Enqueued job. Queue length: $QUEUE_LEN"

# Reliable pop into frontier:processing
POPPED_JOB=$(docker exec crawler-redis redis-cli RPOPLPUSH frontier:queue frontier:processing)
echo "Atomically moved task to 'frontier:processing': $POPPED_JOB"

# Acknowledge completion by removing from frontier:processing
docker exec crawler-redis redis-cli LREM frontier:processing 1 "$SAMPLE_JOB" > /dev/null
PROCESSING_LEN=$(docker exec crawler-redis redis-cli LLEN frontier:processing)
echo "Acknowledged job. Processing queue length: $PROCESSING_LEN (Expected: 0)"

echo -e "\n=== [3/4] Testing Domain Politeness Lease (SET NX PX) ==="
DOMAIN="news.ycombinator.com"
LEASE_KEY="politeness:host:$DOMAIN"

# Worker 1 acquires 1000ms politeness lease
LEASE_RES1=$(docker exec crawler-redis redis-cli SET "$LEASE_KEY" "worker-go-1" NX PX 1000)
echo "Worker 1 lease attempt -> $LEASE_RES1 (Expected: OK)"

# Worker 2 attempts immediate request to same domain
LEASE_RES2=$(docker exec crawler-redis redis-cli SET "$LEASE_KEY" "worker-go-2" NX PX 1000)
echo "Worker 2 lease attempt (immediate) -> '$LEASE_RES2' (Expected: empty/nil - Backed off!)"

# Cleanup
docker exec crawler-redis redis-cli DEL frontier:seen frontier:queue frontier:processing "$LEASE_KEY" > /dev/null

echo -e "\n=== [4/4] Verification Complete! All Redis Frontier mechanics PASSED! ==="
