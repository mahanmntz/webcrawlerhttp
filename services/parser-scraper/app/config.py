import os

class Config:
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # Redis Keys matching REDIS_SPEC.md
    QUEUE_RAW_PAGES: str = "queue:raw_pages"
    QUEUE_FRONTIER: str = "frontier:queue"
    SET_SEEN_URLS: str = "frontier:seen"
    BLOOM_URL_SEEN: str = "frontier:bloom:url"
    QUEUE_PARSED_DOCS: str = "queue:parsed_docs"
    SET_CONTENT_SEEN: str = "content:seen"
    STATS_TOTALS: str = "stats:totals"

    POLL_TIMEOUT_SEC: int = int(os.getenv("POLL_TIMEOUT_SEC", "2"))

    # A raw page not acknowledged within this window is assumed to belong to
    # a dead worker and is redelivered.
    VISIBILITY_TIMEOUT_SEC: int = int(os.getenv("VISIBILITY_TIMEOUT_SEC", "120"))
    MAX_REDELIVERIES: int = int(os.getenv("MAX_REDELIVERIES", "3"))
    REAP_INTERVAL_SEC: int = int(os.getenv("REAP_INTERVAL_SEC", "5"))

    # queue:parsed_docs keeps only the newest documents (0 = unbounded).
    # Use OUTPUT_DIR for a durable copy of every document.
    MAX_PARSED_DOCS: int = int(os.getenv("MAX_PARSED_DOCS", "10000"))
    # When set, every parsed document is also written as Markdown to
    # OUTPUT_DIR/<job_id>/<url hash>.md.
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "")
