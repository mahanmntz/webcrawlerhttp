from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl

class CrawlTarget(BaseModel):
    """
    Payload for a target URL enqueued into the Frontier.
    Maps strictly to shared/contracts/crawl_target.json.
    """
    job_id: str
    url: str
    depth: int = Field(ge=0)
    max_depth: int = Field(ge=0)
    priority: int = Field(default=5, ge=1, le=10)
    created_at: str

class RawPage(BaseModel):
    """
    Raw HTML payload downloaded by the Go crawler.
    Maps strictly to shared/contracts/raw_page.json.
    """
    job_id: str
    url: str
    status_code: int
    content_type: Optional[str] = None
    depth: int
    max_depth: int
    html: str
    duration_ms: int
    fetched_at: str

class ParsedDocument(BaseModel):
    """
    Normalized, structured document extracted from HTML.
    Maps strictly to shared/contracts/parsed_document.json.
    """
    job_id: str
    url: str
    title: str
    meta_description: str = ""
    extracted_links: List[str] = Field(default_factory=list)
    text_sample: str = ""
    parsed_at: str
