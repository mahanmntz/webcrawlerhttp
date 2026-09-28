import logging
from datetime import datetime, timezone
import redis

from app.config import Config
from app.models import RawPage, ParsedDocument, CrawlTarget
from app.extractor import extract_page, body_fingerprint, estimate_tokens

logger = logging.getLogger("ParserPipeline")

# Everything a parsed page writes, applied atomically so that a crash can
# never leave a page half-recorded (e.g. marked as seen content, but with its
# document or child links missing). Reprocessing the same page after a crash
# is therefore harmless: it is simply reported as duplicate content.
#
# The enqueue loop is the same as ENQUEUE_TARGETS_LUA in
# api-gateway/src/frontier.ts. Keep them in sync.
#
# KEYS: content_seen, parsed_docs, bloom, seen_set, frontier, stats, job
# ARGV: fingerprint ('' = skip content dedup), doc_json, raw_bytes, md_bytes,
#       tokens, then (canonical_url, target_json) pairs
# Returns -1 for duplicate content, else the number of links enqueued.
PROCESS_PAGE_LUA = """
local content_seen, parsed_docs, bloom, seen_set, frontier, stats, job =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7]

if ARGV[1] ~= '' and redis.call('SADD', content_seen, ARGV[1]) == 0 then
  redis.call('HINCRBY', job, 'pages_duplicate', 1)
  return -1
end

redis.call('LPUSH', parsed_docs, ARGV[2])
redis.call('HINCRBY', stats, 'documents', 1)
redis.call('HINCRBY', stats, 'raw_html_bytes', ARGV[3])
redis.call('HINCRBY', stats, 'markdown_bytes', ARGV[4])
redis.call('HINCRBY', stats, 'markdown_tokens', ARGV[5])
redis.call('HINCRBY', job, 'pages_parsed', 1)

if redis.call('EXISTS', bloom) == 0 then
  redis.pcall('BF.RESERVE', bloom, '0.001', '1000000')
end

local enqueued = 0
for i = 6, #ARGV, 2 do
  local url, target = ARGV[i], ARGV[i + 1]
  local added = redis.pcall('BF.ADD', bloom, url)
  if type(added) == 'table' and added.err then
    added = redis.call('SADD', seen_set, url)
  end
  if added == 1 then
    redis.call('LPUSH', frontier, target)
    enqueued = enqueued + 1
  end
end

if enqueued > 0 then
  redis.call('HINCRBY', job, 'links_enqueued', enqueued)
end
return enqueued
"""


class ParserPipeline:
    def __init__(self, rdb: redis.Redis):
        self.rdb = rdb
        self._process_page = rdb.register_script(PROCESS_PAGE_LUA)

    def process_raw_page(self, raw_page: RawPage) -> ParsedDocument | None:
        """
        Executes parsing, LLM Markdown generation, link discovery, and re-enqueueing cycle.
        Returns None if the page body duplicates one already indexed.
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

        # 6. Record document, stats and new child targets atomically
        result = int(self._process_page(
            keys=[
                Config.SET_CONTENT_SEEN,
                Config.QUEUE_PARSED_DOCS,
                Config.BLOOM_URL_SEEN,
                Config.SET_SEEN_URLS,
                Config.QUEUE_FRONTIER,
                Config.STATS_TOTALS,
                f"job:{raw_page.job_id}",
            ],
            args=[fingerprint, parsed_doc.model_dump_json(), raw_bytes, md_bytes, estimated_tokens, *link_args],
        ))

        if result < 0:
            logger.info(f"♻️  [DEDUP]  Skipping duplicate content for '{raw_page.url}' (FP: {fingerprint})")
            return None

        title_display = page.title.strip()[:35] if page.title else "(No Title)"
        logger.info(
            f"📄 [LLM_READY] '{raw_page.url}' | Title: '{title_display}' | "
            f"🧠 Tokens: ~{estimated_tokens:,} | 📉 Savings: -{savings_pct}% ({raw_bytes // 1024}KB ➔ {md_bytes // 1024}KB) | "
            f"📥 Next Targets: {result}"
        )

        return parsed_doc
