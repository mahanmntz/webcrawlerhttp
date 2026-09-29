import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import redis

from app.config import Config
from app.models import RawPage, ParsedDocument, CrawlTarget
from app.extractor import extract_page, body_fingerprint, estimate_tokens
from app.reliable_queue import RAW_PAGE_PREFIX
from app.shared_lua import JOBS_LUA, ENQUEUE_TARGETS_LUA

logger = logging.getLogger("ParserPipeline")

# Everything a parsed page changes, applied atomically and fenced on the page
# id still being ours: acknowledgement, content dedup, the document, running
# totals, child targets and job accounting. A crash therefore either leaves
# the page unprocessed (it is redelivered) or fully recorded, and a page is
# never recorded twice.
#
# KEYS: processing, leases, redeliveries, content_seen, parsed_docs, bloom,
#       seen_set, frontier, stats, job, payload_key
# ARGV: page_id ('' = not from the queue, no fencing), fingerprint ('' = skip
#       content dedup), doc_json, raw_bytes, md_bytes, tokens, max_docs,
#       then (canonical_url, target_json) pairs
# Returns -2 if the page was no longer ours, -1 for duplicate content, else
# the number of child links enqueued.
PROCESS_PAGE_LUA = JOBS_LUA + ENQUEUE_TARGETS_LUA + """
local job = KEYS[10]
if ARGV[1] ~= '' then
  if redis.call('LREM', KEYS[1], 1, ARGV[1]) == 0 then
    return -2
  end
  local lease = redis.sha1hex(ARGV[1])
  redis.call('ZREM', KEYS[2], lease)
  redis.call('HDEL', KEYS[3], lease)
  redis.call('DEL', KEYS[11])
end

if ARGV[2] ~= '' and redis.call('SADD', KEYS[4], ARGV[2]) == 0 then
  redis.call('HINCRBY', job, 'pages_duplicate', 1)
  finish_job(job)
  return -1
end

redis.call('LPUSH', KEYS[5], ARGV[3])
local max_docs = tonumber(ARGV[7])
if max_docs > 0 then
  redis.call('LTRIM', KEYS[5], 0, max_docs - 1)
end
redis.call('HINCRBY', KEYS[9], 'documents', 1)
redis.call('HINCRBY', KEYS[9], 'raw_html_bytes', ARGV[4])
redis.call('HINCRBY', KEYS[9], 'markdown_bytes', ARGV[5])
redis.call('HINCRBY', KEYS[9], 'markdown_tokens', ARGV[6])
redis.call('HINCRBY', job, 'pages_parsed', 1)

local enqueued = 0
for _, added in ipairs(enqueue_targets(KEYS[6], KEYS[7], KEYS[8], false, false, ARGV, 8)) do
  enqueued = enqueued + added
end
if enqueued > 0 then
  redis.call('HINCRBY', job, 'links_enqueued', enqueued)
end

-- Children were counted above, so this cannot complete the job prematurely.
finish_job(job)
return enqueued
"""

STALE = -2
DUPLICATE = -1


class ParserPipeline:
    def __init__(self, rdb: redis.Redis, output_dir: str | None = None):
        self.rdb = rdb
        self._process_page = rdb.register_script(PROCESS_PAGE_LUA)
        self.output_dir = Path(output_dir) if output_dir else None

    def process_raw_page(
        self,
        raw_page: RawPage,
        page_id: str = "",
        settle_keys: list[str] | None = None,
    ) -> ParsedDocument | None:
        """
        Executes parsing, LLM Markdown generation, link discovery, and re-enqueueing cycle.

        page_id/settle_keys identify the queue item being processed, so the
        commit also acknowledges it (fenced). Returns None if the page body
        duplicates one already indexed, or if the page was no longer ours.
        """
        # 1. Parse DOM once: Markdown, text, fingerprint input and child links
        page = extract_page(
            raw_page.html,
            raw_page.url,
            stay_in_domain=raw_page.stay_in_domain,
            scope_host=raw_page.scope_host or None,
        )

        # 2. Pages without text (e.g. JS-rendered shells) all normalize to "",
        #    so they are exempt from content dedup instead of colliding.
        fingerprint = str(body_fingerprint(page.fingerprint_text)) if page.fingerprint_text else ""

        # 3. Calculate Token & Size Analytics
        raw_bytes = len(raw_page.html.encode("utf-8"))
        md_bytes = len(page.markdown.encode("utf-8"))
        estimated_tokens = estimate_tokens(page.markdown)
        savings_pct = round((1 - (md_bytes / max(1, raw_bytes))) * 100, 1)

        # 4. Construct ParsedDocument
        parsed_doc = ParsedDocument(
            job_id=raw_page.job_id,
            url=raw_page.url,
            title=page.title,
            meta_description=page.meta_description,
            extracted_links=page.links,
            text_sample=page.text_sample,
            markdown=page.markdown,
            estimated_tokens=estimated_tokens,
            raw_html_bytes=raw_bytes,
            markdown_bytes=md_bytes,
            token_savings_pct=savings_pct,
            parsed_at=datetime.now(timezone.utc).isoformat()
        )

        # 5. Child targets, if the next level is still within max_depth
        next_depth = raw_page.depth + 1
        link_args: list[str] = []
        if next_depth <= raw_page.max_depth:
            now = datetime.now(timezone.utc).isoformat()
            for link in page.links:
                target = CrawlTarget(
                    job_id=raw_page.job_id,
                    url=link,
                    depth=next_depth,
                    max_depth=raw_page.max_depth,
                    priority=5,
                    stay_in_domain=raw_page.stay_in_domain,
                    scope_host=raw_page.scope_host,
                    created_at=now,
                )
                link_args += [link, target.model_dump_json()]

        # 6. Durable Markdown sink, written before the commit so that a crash
        #    in between leads to a rewrite on redelivery, never a lost file.
        sink_path = self._write_markdown(parsed_doc)

        # 7. Record everything atomically (and acknowledge the page)
        settle_keys = settle_keys or ["", "", ""]
        result = int(self._process_page(
            keys=[
                *settle_keys,
                Config.SET_CONTENT_SEEN,
                Config.QUEUE_PARSED_DOCS,
                Config.BLOOM_URL_SEEN,
                Config.SET_SEEN_URLS,
                Config.QUEUE_FRONTIER,
                Config.STATS_TOTALS,
                f"job:{raw_page.job_id}",
                RAW_PAGE_PREFIX + page_id,
            ],
            args=[
                page_id, fingerprint, parsed_doc.model_dump_json(),
                raw_bytes, md_bytes, estimated_tokens, Config.MAX_PARSED_DOCS,
                *link_args,
            ],
        ))

        if result == STALE:
            # Redelivered to another worker, which will commit it (and write
            # the same file path).
            logger.warning(f"⏱️  [STALE]  Lease lost for '{raw_page.url}'; discarding this result")
            return None

        if result == DUPLICATE:
            if sink_path:
                sink_path.unlink(missing_ok=True)
            logger.info(f"♻️  [DEDUP]  Skipping duplicate content for '{raw_page.url}' (FP: {fingerprint})")
            return None

        title_display = page.title.strip()[:35] if page.title else "(No Title)"
        logger.info(
            f"📄 [LLM_READY] '{raw_page.url}' | Title: '{title_display}' | "
            f"🧠 Tokens: ~{estimated_tokens:,} | 📉 Savings: -{savings_pct}% ({raw_bytes // 1024}KB ➔ {md_bytes // 1024}KB) | "
            f"📥 Next Targets: {result}"
        )

        return parsed_doc

    def _write_markdown(self, doc: ParsedDocument) -> Path | None:
        """Writes <output_dir>/<job_id>/<url hash>.md atomically. Idempotent per URL."""
        if not self.output_dir:
            return None

        directory = self.output_dir / doc.job_id
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{hashlib.sha1(doc.url.encode('utf-8')).hexdigest()[:16]}.md"

        title = doc.title.replace('"', '\\"')
        content = (
            "---\n"
            f"url: {doc.url}\n"
            f'title: "{title}"\n'
            f"job_id: {doc.job_id}\n"
            f"parsed_at: {doc.parsed_at}\n"
            f"estimated_tokens: {doc.estimated_tokens}\n"
            "---\n\n"
            f"{doc.markdown}\n"
        )
        tmp = path.with_suffix(f".md.tmp{os.getpid()}")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
        return path
