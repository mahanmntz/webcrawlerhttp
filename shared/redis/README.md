# Shared Redis Lua

Lua that more than one service runs. Each service embeds these snippets verbatim
(Docker build contexts are per service, so the files can't be loaded at runtime),
and each service's test suite fails if its embedded copy differs from the file here.

| File | Defines | Embedded in |
|---|---|---|
| `jobs.lua` | `job_key_from_json`, `job_key_from_id`, `finish_job` | crawler-engine, parser-scraper |
| `enqueue_targets.lua` | `enqueue_targets` | api-gateway, parser-scraper |
| `reap.lua` | reaper script body (uses `jobs.lua`) | crawler-engine, parser-scraper |

Edit the file here first, then paste it into the constants the tests point to.
