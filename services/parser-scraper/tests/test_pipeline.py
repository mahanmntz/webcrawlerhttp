from unittest.mock import MagicMock
from app.models import RawPage
from app.pipeline import ParserPipeline

def test_pipeline_content_deduplication():
    mock_redis = MagicMock()
    # First time content seen returns 1 (new), second time returns 0 (duplicate)
    mock_redis.sadd.side_effect = [1, 0]
    mock_redis.execute_command.return_value = 1

    pipeline = ParserPipeline(mock_redis)

    raw_page = RawPage(
        job_id="job-123",
        url="https://example.com/page1",
        html="<html><head><title>Test</title></head><body>Hello World</body></html>",
        depth=0,
        max_depth=2,
        status_code=200,
        duration_ms=50,
        fetched_at="2026-09-28T00:00:00Z"
    )

    # First call: new content, processes successfully
    doc = pipeline.process_raw_page(raw_page)
    assert doc is not None
    assert doc.title == "Test"

    # Second call with same content: duplicate detected, returns None
    doc_dup = pipeline.process_raw_page(raw_page)
    assert doc_dup is None

def test_pipeline_bloom_fallback_and_link_discovery():
    mock_redis = MagicMock()
    # Mock content seen as new
    mock_redis.sadd.return_value = 1
    # Mock Bloom filter command: return 1 for new link
    mock_redis.execute_command.return_value = 1

    pipeline = ParserPipeline(mock_redis)

    html = """
    <html>
      <head><title>Parent Page</title></head>
      <body>
        <a href="https://example.com/child-1">Child 1</a>
        <a href="https://example.com/child-2">Child 2</a>
      </body>
    </html>
    """
    raw_page = RawPage(
        job_id="job-456",
        url="https://example.com/parent",
        html=html,
        depth=1,
        max_depth=2,
        status_code=200,
        duration_ms=80,
        fetched_at="2026-09-28T00:00:00Z"
    )

    doc = pipeline.process_raw_page(raw_page)
    assert doc is not None
    assert len(doc.extracted_links) == 2
    # Verify LPUSH was called for parsed doc + 2 child crawl targets
    assert mock_redis.lpush.call_count == 3
