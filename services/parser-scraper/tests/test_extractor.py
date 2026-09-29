import pytest
from app.extractor import canonicalize_url, extract_content, html_to_markdown, estimate_tokens

def test_canonicalize_url_relative_resolution():
    base = "https://example.com/articles/index.html"
    
    assert canonicalize_url(base, "/about") == "https://example.com/about"
    assert canonicalize_url(base, "team.html") == "https://example.com/articles/team.html"
    assert canonicalize_url(base, "../contact") == "https://example.com/contact"

def test_canonicalize_url_fragment_stripping():
    base = "https://example.com"
    assert canonicalize_url(base, "/page#heading-2") == "https://example.com/page"

def test_canonicalize_url_ignored_schemes_and_extensions():
    base = "https://example.com"
    
    assert canonicalize_url(base, "javascript:void(0)") is None
    assert canonicalize_url(base, "mailto:info@example.com") is None
    assert canonicalize_url(base, "tel:+123456789") is None
    
    # Asset extensions
    assert canonicalize_url(base, "/images/logo.png") is None
    assert canonicalize_url(base, "/downloads/report.pdf") is None
    assert canonicalize_url(base, "/assets/app.js") is None

def test_canonicalize_url_domain_guard():
    base = "https://docs.python.org/3/tutorial/"
    
    # In-domain links
    assert canonicalize_url(base, "/3/library/os.html", stay_in_domain=True) == "https://docs.python.org/3/library/os.html"
    assert canonicalize_url(base, "https://docs.python.org/3/faq.html", stay_in_domain=True) == "https://docs.python.org/3/faq.html"
    
    # Out-of-domain links guarded
    assert canonicalize_url(base, "https://github.com/python/cpython", stay_in_domain=True) is None
    assert canonicalize_url(base, "https://twitter.com/ThePSF", stay_in_domain=True) is None

def test_html_to_markdown_clean_conversion():
    html = """
    <html>
      <head><style>.ad { display:none; }</style></head>
      <body>
        <nav><a href="/menu">Menu</a></nav>
        <h1>Distributed Systems Guide</h1>
        <p>This is a paragraph with <code>inline code</code> and a <a href="https://example.com">link</a>.</p>
        <pre>def scrape(): pass</pre>
        <ul>
          <li>Point 1</li>
          <li>Point 2</li>
        </ul>
        <footer>Copyright 2026</footer>
      </body>
    </html>
    """
    md = html_to_markdown(html)
    assert "# Distributed Systems Guide" in md
    assert "Menu" not in md
    assert "Copyright" not in md
    assert "`inline code`" in md
    assert "```\ndef scrape(): pass\n```" in md
    assert "- Point 1" in md
    assert estimate_tokens(md) > 0

def test_extract_content_full_flow():
    sample_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Alex Xu System Design Notes</title>
        <meta name="description" content="Deep dive into web crawler architecture.">
        <style>body { color: red; }</style>
        <script>console.log("secret tracker");</script>
    </head>
    <body>
        <h1>Web Crawler Engine</h1>
        <p>This is the main article content about distributed scraping.</p>
        <a href="/chapter-9/frontier">Chapter 9 Frontier</a>
        <a href="https://other.com/resources">Outbound Link</a>
        <a href="/images/diagram.png">Ignore Image</a>
        <a href="#section-top">Ignore Anchor</a>
    </body>
    </html>
    """

    base_url = "https://alexxu.com/books"
    title, meta_desc, text_sample, markdown, links = extract_content(sample_html, base_url, stay_in_domain=False)

    assert title == "Alex Xu System Design Notes"
    assert meta_desc == "Deep dive into web crawler architecture."
    assert "secret tracker" not in text_sample
    assert "Web Crawler Engine" in text_sample
    assert "# Web Crawler Engine" in markdown
    
    # Check extracted canonical links
    assert "https://alexxu.com/chapter-9/frontier" in links
    assert "https://other.com/resources" in links
    assert len([link for link in links if link.endswith(".png")]) == 0


def test_token_reduction_strips_wikipedia_noise_and_hidden_elements():
    html = """
    <html>
      <body>
        <div id="toc" class="toc"><h2>Contents</h2><ul><li>1 Intro</li></ul></div>
        <div class="infobox"><table><tr><td>Ignored sidebar stats</td></tr></table></div>
        <div style="display: none">Hidden tracking text</div>
        <span aria-hidden="true">Hidden icon text</span>
        <p hidden>Invisible secret paragraph</p>
        <div class="sr-only">Screen reader duplicate</div>
        <main>
          <h1>Artificial Intelligence</h1>
          <p>AI is intelligence demonstrated by machines<sup>[1]</sup><sup>[citation needed]</sup>.</p>
          <span class="mw-editsection"><a href="#">[edit]</a></span>
          <div class="reflist"><ol><li>Reference 1</li></ol></div>
        </main>
      </body>
    </html>
    """
    md = html_to_markdown(html)
    assert "# Artificial Intelligence" in md
    assert "AI is intelligence demonstrated by machines." in md
    # Assert noise is completely stripped
    assert "Contents" not in md
    assert "Ignored sidebar stats" not in md
    assert "Hidden tracking text" not in md
    assert "Hidden icon text" not in md
    assert "Invisible secret paragraph" not in md
    assert "Screen reader duplicate" not in md
    assert "[1]" not in md
    assert "citation needed" not in md
    assert "[edit]" not in md
    assert "Reference 1" not in md


def test_table_to_gfm_markdown_conversion():
    html = """
    <article>
      <h1>Benchmark Results</h1>
      <table>
        <tr><th>Model</th><th>Tokens</th><th>Speed</th></tr>
        <tr><td>SpiderRAG</td><td>150</td><td>0.1s</td></tr>
        <tr><td>Standard</td><td>1500</td><td>1.2s</td></tr>
      </table>
    </article>
    """
    md = html_to_markdown(html)
    assert "| Model | Tokens | Speed |" in md
    assert "| --- | --- | --- |" in md
    assert "| SpiderRAG | 150 | 0.1s |" in md
    assert "| Standard | 1500 | 1.2s |" in md


def test_compact_links_preserves_external_and_cleans_internal():
    html = """
    <article>
      <p>Read the <a href="/docs/guide">internal guide</a> or visit <a href="https://github.com/mahanmntz">our GitHub</a>.</p>
    </article>
    """
    md = html_to_markdown(html)
    # Internal link is stripped of redundant URL markup
    assert "internal guide" in md
    assert "](/docs/guide)" not in md
    # External link preserves markdown link
    assert "[our GitHub](https://github.com/mahanmntz)" in md


def test_wikipedia_page_with_vector_toc_root_class():
    html = """
    <html class="client-nojs vector-feature-toc-pinned-clientpref-1 vector-toc-available">
      <head><title>Web Crawler - Wikipedia</title></head>
      <body>
        <div id="content" class="mw-body">
          <div id="mw-content-text">
            <h1>Web Crawler</h1>
            <div id="toc" class="toc"><h2>Contents</h2><ul><li>1 Scope</li></ul></div>
            <p>A web crawler is an Internet bot that systematically browses the World Wide Web.</p>
            <div class="reflist"><ol><li>Reference 1</li></ol></div>
          </div>
        </div>
      </body>
    </html>
    """
    md = html_to_markdown(html)
    assert "# Web Crawler" in md
    assert "A web crawler is an Internet bot that systematically browses the World Wide Web." in md
    assert "Contents" not in md
    assert "Reference 1" not in md
    assert estimate_tokens(md) > 10


def test_detect_language():
    from app.extractor import detect_language
    from bs4 import BeautifulSoup

    soup_en = BeautifulSoup('<html lang="en"><body>Hello world</body></html>', 'html.parser')
    assert detect_language(soup_en, "Hello world") == "en"

    soup_fa = BeautifulSoup('<html lang="fa-IR"><body>سلام دنیا</body></html>', 'html.parser')
    assert detect_language(soup_fa, "سلام دنیا") == "fa"

    soup_heuristic = BeautifulSoup('<html><body>هوش مصنوعی و یادگیری ماشین</body></html>', 'html.parser')
    assert detect_language(soup_heuristic, "هوش مصنوعی و یادگیری ماشین") == "fa"


def test_extract_rich_metadata():
    from app.extractor import extract_rich_metadata
    from bs4 import BeautifulSoup

    html = """
    <html>
      <head>
        <meta property="og:title" content="Advanced System Design" />
        <meta property="og:description" content="A comprehensive guide to distributed systems." />
        <meta name="author" content="Alex Xu" />
        <script type="application/ld+json">
        {
          "@type": "Article",
          "headline": "Advanced System Design",
          "datePublished": "2026-09-29"
        }
        </script>
      </head>
      <body>Content</body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    meta = extract_rich_metadata(soup, "https://example.com/guide")
    assert meta["og_title"] == "Advanced System Design"
    assert meta["og_description"] == "A comprehensive guide to distributed systems."
    assert meta["author"] == "Alex Xu"
    assert meta["schema_type"] == "Article"
    assert meta["published_time"] == "2026-09-29"


def test_chunk_markdown_semantic_split():
    from app.extractor import chunk_markdown

    md = """
# System Design

This is the introduction to system design.

## Distributed Caching

Redis and Memcached are popular in-memory key-value caches.
They provide ultra-low latency data access.

## Message Queues

Kafka and RabbitMQ handle asynchronous message passing.
They decouple producers and consumers effectively.
"""
    chunks = chunk_markdown(md, doc_url="https://example.com/sys", doc_title="System Design", max_tokens=100, overlap_tokens=20)
    assert len(chunks) >= 3
    assert chunks[0]["title"] == "System Design"
    assert "System Design" in chunks[0]["section"] or "Overview" in chunks[0]["section"]
    assert any("Distributed Caching" in c["section"] for c in chunks)
    assert any("Message Queues" in c["section"] for c in chunks)
    for c in chunks:
        assert "chunk_id" in c
        assert c["estimated_tokens"] > 0



