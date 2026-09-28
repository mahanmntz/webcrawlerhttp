import json
import logging
from datetime import datetime, timezone
import redis

from app.config import Config
from app.models import RawPage, ParsedDocument, CrawlTarget
from app.extractor import extract_content, extract_clean_body_text, body_fingerprint

logger = logging.getLogger("ParserPipeline")

class ParserPipeline:
    def __init__(self, rdb: redis.Redis):
        self.rdb = rdb

    def process_raw_page(self, raw_page: RawPage) -> ParsedDocument | None:
        """
        Executes the full parsing, link discovery, content deduplication, and re-enqueueing cycle.
        """
        # 1. Parse DOM & Extract Links
        title, meta_desc, text_sample, links = extract_content(raw_page.html, raw_page.url)
        normalized_body = extract_clean_body_text(raw_page.html)
        fingerprint = body_fingerprint(normalized_body) if normalized_body else body_fingerprint("")

        # 2. Skip duplicate content to suppress mirror pages under different URLs
        is_new_content = self.rdb.sadd(Config.SET_CONTENT_SEEN, str(fingerprint))
        if is_new_content == 0:
            logger.info(f"Skipping duplicate content for '{raw_page.url}' (fingerprint={fingerprint})")
            return None

        # 3. Construct ParsedDocument
        parsed_doc = ParsedDocument(
            job_id=raw_page.job_id,
            url=raw_page.url,
            title=title,
            meta_description=meta_desc,
            extracted_links=links,
            text_sample=text_sample,
            parsed_at=datetime.now(timezone.utc).isoformat()
        )

        # 4. Store structured document in output queue
        self.rdb.lpush(Config.QUEUE_PARSED_DOCS, parsed_doc.model_dump_json())

        # 5. Filter & Re-enqueue new links into Frontier if within max_depth
        next_depth = raw_page.depth + 1
        new_enqueued_count = 0

        if next_depth <= raw_page.max_depth:
            for link in links:
                # O(1) Atomic Deduplication Check in Redis
                is_new = self.rdb.sadd(Config.SET_SEEN_URLS, link)
                if is_new == 1:
                    # Brand new link discovered! Package as CrawlTarget contract
                    target = CrawlTarget(
                        job_id=raw_page.job_id,
                        url=link,
                        depth=next_depth,
                        max_depth=raw_page.max_depth,
                        priority=5,
                        created_at=datetime.now(timezone.utc).isoformat()
                    )
                    self.rdb.lpush(Config.QUEUE_FRONTIER, target.model_dump_json())
                    new_enqueued_count += 1
                else:
                    logger.debug(f"Link deduplicated (already seen): {link}")

        logger.info(
            f"Parsed '{raw_page.url}' | Title: '{title[:40]}' | "
            f"Links Found: {len(links)} | New Targets Enqueued: {new_enqueued_count} (Depth {next_depth}/{raw_page.max_depth}) | "
            f"Content FP: {fingerprint}"
        )

        return parsed_doc
