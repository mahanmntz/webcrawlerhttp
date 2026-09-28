from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

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
    stay_in_domain: bool = True
    scope_host: str = ""
    attempts: int = 0
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
    stay_in_domain: bool = True
    scope_host: str = ""
    html: str
    duration_ms: int
    fetched_at: str

class ParsedDocument(BaseModel):
    """
    Normalized, structured document extracted from HTML, including LLM-ready Markdown.
    Maps strictly to shared/contracts/parsed_document.json.
    """
    job_id: str
    url: str
    title: str
    meta_description: str = ""
    extracted_links: List[str] = Field(default_factory=list)
    text_sample: str = ""
    markdown: str = ""
    estimated_tokens: int = 0
    raw_html_bytes: int = 0
    markdown_bytes: int = 0
    token_savings_pct: float = 0.0
    parsed_at: str
