"""The payloads this service produces must satisfy shared/contracts/*.json."""
import json
import uuid
from pathlib import Path

import pytest
from jsonschema import Draft7Validator, FormatChecker

from app.config import Config
from app.models import RawPage
from app.pipeline import ParserPipeline

CONTRACTS = Path(__file__).resolve().parents[3] / "shared/contracts"


def validator(name: str) -> Draft7Validator:
    schema = json.loads((CONTRACTS / name).read_text())
    return Draft7Validator(schema, format_checker=FormatChecker())


@pytest.fixture
def produced(rdb):
    job_id = str(uuid.uuid4())
    page = RawPage(
        job_id=job_id,
        url="https://example.com/start",
        status_code=200,
        content_type="text/html",
        depth=0,
        max_depth=1,
        stay_in_domain=True,
        scope_host="example.com",
        html='<html><head><title>T</title><meta name="description" content="d"></head>'
             '<body><h1>Hi</h1><a href="/next">n</a></body></html>',
        duration_ms=5,
        fetched_at="2026-09-29T00:00:00Z",
    )
    ParserPipeline(rdb).process_raw_page(page)
    return {
        "targets": rdb.lrange(Config.QUEUE_FRONTIER, 0, -1),
        "docs": rdb.lrange(Config.QUEUE_PARSED_DOCS, 0, -1),
    }


def test_child_targets_match_crawl_target_contract(produced):
    assert produced["targets"]
    for raw in produced["targets"]:
        validator("crawl_target.json").validate(json.loads(raw))


def test_parsed_documents_match_contract(produced):
    assert produced["docs"]
    for raw in produced["docs"]:
        validator("parsed_document.json").validate(json.loads(raw))


def test_raw_page_contract_accepts_what_the_model_accepts():
    # The parser's input model and the contract must agree on a valid page.
    page = {
        "job_id": str(uuid.uuid4()), "url": "https://example.com/", "status_code": 200,
        "content_type": "text/html", "depth": 0, "max_depth": 1, "stay_in_domain": True,
        "scope_host": "example.com", "html": "<p>x</p>", "duration_ms": 1,
        "fetched_at": "2026-09-29T00:00:00Z",
    }
    validator("raw_page.json").validate(page)
    RawPage.model_validate(page)
