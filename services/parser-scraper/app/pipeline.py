import json
import logging
from datetime import datetime, timezone
import redis

from app.config import Config
from app.models import RawPage, ParsedDocument, CrawlTarget
from app.extractor import (
    extract_content,
    extract_clean_body_text,
    body_fingerprint,
    estimate_tokens
)

logger = logging.getLogger("ParserPipeline")


class ParserPipeline:
    def __init__(self, rdb: redis.Redis):
        self.rdb = rdb

    def _is_new_url(self, url: str) -> bool:
        """
        Check if URL is new using RedisBloom filter with graceful fallback to standard Set.
        """
        try:
            return bool(self.rdb.execute_command("BF.ADD", Config.BLOOM_URL_SEEN, url) == 1)
        except Exception:
            return bool(self.rdb.sadd(Config.SET_SEEN_URLS, url) == 1)

    def process_raw_page(self, raw_page: RawPage) -> ParsedDocument | None:
        """
        Executes parsing, LLM Markdown generation, link discovery, and re-enqueueing cycle.
        """
        stay_in_domain = getattr(raw_page, "stay_in_domain", True)

        # 1. Parse DOM, Extract Markdown & Child Links
        title, meta_desc, text_sample, markdown, links = extract_content(
            raw_page.html,
            raw_page.url,
            stay_in_domain=stay_in_domain
        )
        normalized_body = extract_clean_body_text(raw_page.html)
        fingerprint = body_fingerprint(normalized_body) if normalized_body else body_fingerprint("")

        # 2. Skip duplicate content to suppress mirror pages under different URLs
        is_new_content = self.rdb.sadd(Config.SET_CONTENT_SEEN, str(fingerprint))
        if is_new_content == 0:
            logger.info(f"♻️  [DEDUP]  Skipping duplicate content for '{raw_page.url}' (FP: {fingerprint})")
            return None

        # 3. Calculate Token & Size Analytics
        raw_bytes = len(raw_page.html.encode("utf-8"))
        md_bytes = len(markdown.encode("utf-8"))
        estimated_tokens = estimate_tokens(markdown)
        savings_pct = round((1 - (md_bytes / max(1, raw_bytes))) * 100, 1)

        # 4. Construct ParsedDocument
        parsed_doc = ParsedDocument(
            job_id=raw_page.job_id,
            url=raw_page.url,
            title=title,
            meta_description=meta_desc,
            extracted_links=links,
            text_sample=text_sample,
            markdown=markdown,
            estimated_tokens=estimated_tokens,
            raw_html_bytes=raw_bytes,
            markdown_bytes=md_bytes,
            token_savings_pct=savings_pct,
            parsed_at=datetime.now(timezone.utc).isoformat()
        )

        # 5. Store structured document in output queue
        self.rdb.lpush(Config.QUEUE_PARSED_DOCS, parsed_doc.model_dump_json())

        # 6. Filter & Re-enqueue new links into Frontier if within max_depth
        next_depth = raw_page.depth + 1
        new_enqueued_count = 0

        if next_depth <= raw_page.max_depth:
            for link in links:
                if self._is_new_url(link):
                    target = CrawlTarget(
                        job_id=raw_page.job_id,
                        url=link,
                        depth=next_depth,
                        max_depth=raw_page.max_depth,
                        priority=5,
                        stay_in_domain=stay_in_domain,
                        created_at=datetime.now(timezone.utc).isoformat()
                    )
                    self.rdb.lpush(Config.QUEUE_FRONTIER, target.model_dump_json())
                    new_enqueued_count += 1
                else:
                    logger.debug(f"Link deduplicated: {link}")

        title_display = title.strip()[:35] if title else "(No Title)"
        logger.info(
            f"📄 [LLM_READY] '{raw_page.url}' | Title: '{title_display}' | "
            f"🧠 Tokens: ~{estimated_tokens:,} | 📉 Savings: -{savings_pct}% ({raw_bytes // 1024}KB ➔ {md_bytes // 1024}KB) | "
            f"📥 Next Targets: {new_enqueued_count}"
        )

        return parsed_doc
