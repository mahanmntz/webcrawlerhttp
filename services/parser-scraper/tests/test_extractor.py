import pytest
from app.extractor import canonicalize_url, extract_content

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
    title, meta_desc, text_sample, links = extract_content(sample_html, base_url)

    assert title == "Alex Xu System Design Notes"
    assert meta_desc == "Deep dive into web crawler architecture."
    assert "secret tracker" not in text_sample
    assert "Web Crawler Engine" in text_sample
    
    # Check extracted canonical links
    assert "https://alexxu.com/chapter-9/frontier" in links
    assert "https://other.com/resources" in links
    assert len([link for link in links if link.endswith(".png")]) == 0
