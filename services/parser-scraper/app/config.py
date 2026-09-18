import os

class Config:
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    
    # Redis Keys matching REDIS_SPEC.md
    QUEUE_RAW_PAGES: str = "queue:raw_pages"
    QUEUE_FRONTIER: str = "frontier:queue"
    SET_SEEN_URLS: str = "frontier:seen"
    QUEUE_PARSED_DOCS: str = "queue:parsed_docs"

    POLL_TIMEOUT_SEC: int = int(os.getenv("POLL_TIMEOUT_SEC", "2"))
