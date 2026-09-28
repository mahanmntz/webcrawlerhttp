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
