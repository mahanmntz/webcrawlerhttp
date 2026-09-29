import json

from app.config import Config
from app.models import RawPage
from app.pipeline import ParserPipeline


def make_page(**overrides) -> RawPage:
    fields = dict(
        job_id="job-123",
        url="https://example.com/page1",
        html="<html><head><title>Test</title></head><body><h1>Hello World</h1><p>Test body</p></body></html>",
        depth=0,
        max_depth=2,
        status_code=200,
        duration_ms=50,
        fetched_at="2026-09-28T00:00:00Z",
    )
    fields.update(overrides)
    return RawPage(**fields)


def test_pipeline_content_deduplication(rdb):
    pipeline = ParserPipeline(rdb)
    raw_page = make_page()

    # First call: new content, processes successfully
    doc = pipeline.process_raw_page(raw_page)
    assert doc is not None
    assert doc.title == "Test"
    assert "# Hello World" in doc.markdown
    assert doc.estimated_tokens > 0
    assert doc.token_savings_pct > 0

    # Second call with same content: duplicate detected, returns None
    assert pipeline.process_raw_page(raw_page) is None
    assert rdb.llen(Config.QUEUE_PARSED_DOCS) == 1
    assert rdb.hget("job:job-123", "pages_duplicate") == "1"


def test_pipeline_mirrors_with_different_titles_are_duplicates(rdb):
    pipeline = ParserPipeline(rdb)
    body = "<body><h1>Mirror</h1><p>Same body text.</p></body>"

    assert pipeline.process_raw_page(make_page(url="https://a.example/", html=f"<head><title>A</title></head>{body}")) is not None
    assert pipeline.process_raw_page(make_page(url="https://b.example/", html=f"<head><title>B</title></head>{body}")) is None


def test_pipeline_pages_without_text_are_not_treated_as_duplicates(rdb):
    pipeline = ParserPipeline(rdb)
    shell = '<html><body><div id="root"></div><script src="/app.js"></script></body></html>'

    assert pipeline.process_raw_page(make_page(url="https://a.example/", html=shell)) is not None
    assert pipeline.process_raw_page(make_page(url="https://b.example/", html=shell)) is not None
    assert rdb.scard(Config.SET_CONTENT_SEEN) == 0


def test_pipeline_link_discovery_scope_and_dedup(rdb):
    pipeline = ParserPipeline(rdb)
    html = """
    <html>
      <head><title>Parent Page</title></head>
      <body>
        <a href="https://example.com/child-1">Child 1</a>
        <a href="/child-2#section">Child 2</a>
        <a href="https://blog.example.com/post">Subdomain</a>
        <a href="https://github.com/example">External</a>
      </body>
    </html>
    """
    # Redirected onto www.: the boundary is still the seed's host.
    raw_page = make_page(
        job_id="job-456",
        url="https://www.example.com/parent",
        html=html,
        depth=1,
        stay_in_domain=True,
        scope_host="example.com",
    )

    doc = pipeline.process_raw_page(raw_page)
    assert doc is not None
    assert doc.extracted_links == [
        "https://blog.example.com/post",
        "https://example.com/child-1",
        "https://www.example.com/child-2",
    ]

    targets = [json.loads(t) for t in rdb.lrange(Config.QUEUE_FRONTIER, 0, -1)]
    assert {t["url"] for t in targets} == set(doc.extracted_links)
    assert all(t["depth"] == 2 and t["scope_host"] == "example.com" for t in targets)
    assert rdb.hget("job:job-456", "links_enqueued") == "3"

    # Links discovered again from another page are not re-enqueued.
    pipeline.process_raw_page(raw_page.model_copy(update={"url": "https://www.example.com/other", "html": html + "<p>x</p>"}))
    assert rdb.llen(Config.QUEUE_FRONTIER) == 3


def test_pipeline_respects_max_depth(rdb):
    pipeline = ParserPipeline(rdb)
    page = make_page(html='<a href="/next">next</a>', depth=2, max_depth=2)

    assert pipeline.process_raw_page(page) is not None
    assert rdb.llen(Config.QUEUE_FRONTIER) == 0


def test_pipeline_maintains_running_totals(rdb):
    pipeline = ParserPipeline(rdb)
    first = pipeline.process_raw_page(make_page(url="https://a.example/", html="<p>alpha content</p>"))
    second = pipeline.process_raw_page(make_page(url="https://b.example/", html="<p>beta content here</p>"))

    totals = rdb.hgetall(Config.STATS_TOTALS)
    assert int(totals["documents"]) == 2
    assert int(totals["raw_html_bytes"]) == first.raw_html_bytes + second.raw_html_bytes
    assert int(totals["markdown_tokens"]) == first.estimated_tokens + second.estimated_tokens


def test_pipeline_completes_job_when_last_page_has_no_children(rdb):
    rdb.hset("job:job-123", mapping={"status": "running", "outstanding": 1})
    pipeline = ParserPipeline(rdb)

    pipeline.process_raw_page(make_page(depth=2, max_depth=2))

    assert rdb.hget("job:job-123", "status") == "completed"
    assert rdb.hget("job:job-123", "outstanding") == "0"


def test_pipeline_children_keep_job_open(rdb):
    rdb.hset("job:job-123", mapping={"status": "running", "outstanding": 1})
    pipeline = ParserPipeline(rdb)

    pipeline.process_raw_page(make_page(html='<p>parent</p><a href="/a">a</a><a href="/b">b</a>'))

    assert rdb.hget("job:job-123", "outstanding") == "2"
    assert rdb.hget("job:job-123", "status") == "running"


def test_pipeline_caps_parsed_docs_list(rdb, monkeypatch):
    monkeypatch.setattr(Config, "MAX_PARSED_DOCS", 2)
    pipeline = ParserPipeline(rdb)
    for i in range(3):
        pipeline.process_raw_page(make_page(url=f"https://e.example/{i}", html=f"<p>page {i}</p>"))

    assert rdb.llen(Config.QUEUE_PARSED_DOCS) == 2
    assert rdb.hget(Config.STATS_TOTALS, "documents") == "3", "totals count every document"


def test_pipeline_writes_markdown_sink_and_removes_duplicates(rdb, tmp_path):
    pipeline = ParserPipeline(rdb, output_dir=str(tmp_path))
    body = "<body><h1>Guide</h1><p>Same text.</p></body>"

    pipeline.process_raw_page(make_page(url="https://a.example/", html=body))
    pipeline.process_raw_page(make_page(url="https://b.example/", html=body))

    files = list((tmp_path / "job-123").glob("*.md"))
    assert len(files) == 1, "duplicate content must not leave a file behind"
    text = files[0].read_text()
    assert text.startswith("---\nurl: https://a.example/\n")
    assert "# Guide" in text
