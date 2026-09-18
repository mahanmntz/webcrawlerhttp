import re
from typing import List, Tuple
from urllib.parse import urljoin, urlparse, urldefrag
from bs4 import BeautifulSoup

IGNORED_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".pdf", ".zip", ".tar", ".gz", ".rar", ".7z",
    ".mp3", ".mp4", ".avi", ".mov", ".wav",
    ".css", ".js", ".json", ".xml", ".ico", ".woff", ".woff2"
}

def canonicalize_url(base_url: str, raw_href: str) -> str | None:
    """
    Normalizes a discovered link according to crawler standards:
    1. Resolves relative URLs to absolute.
    2. Strips URL fragments (#hash).
    3. Validates HTTP/HTTPS schemes.
    4. Filters out non-HTML assets (images, archives, media).
    """
    if not raw_href or raw_href.startswith(("javascript:", "mailto:", "tel:", "#")):
        return None

    # Resolve relative URL against base page URL
    absolute_url = urljoin(base_url, raw_href.strip())

    # Strip fragments (#section-1)
    defragged, _ = urldefrag(absolute_url)

    parsed = urlparse(defragged)
    if parsed.scheme not in ("http", "https"):
        return None

    if not parsed.netloc:
        return None

    # Check file extension
    path_lower = parsed.path.lower()
    for ext in IGNORED_EXTENSIONS:
        if path_lower.endswith(ext):
            return None

    return defragged

def extract_content(html: str, base_url: str) -> Tuple[str, str, str, List[str]]:
    """
    Parses HTML DOM tree:
    - Extracts page <title>
    - Extracts <meta name="description">
    - Extracts clean body text sample (stripping scripts/styles)
    - Discovers and canonicalizes all outbound <a href> links
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1. Extract Title
    title = ""
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        title = title_tag.string.strip()

    # 2. Extract Meta Description
    meta_desc = ""
    meta_tag = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
    if meta_tag and meta_tag.get("content"):
        meta_desc = meta_tag["content"].strip()

    # 3. Clean Text Sample (remove scripts, styles, metadata)
    for element in soup(["script", "style", "noscript", "header", "footer"]):
        element.decompose()

    raw_text = soup.get_text(separator=" ", strip=True)
    text_sample = raw_text[:500] if raw_text else ""

    # 4. Extract Outbound Links
    extracted_links = set()
    for a_tag in soup.find_all("a", href=True):
        canonical = canonicalize_url(base_url, a_tag["href"])
        if canonical and canonical != base_url:
            extracted_links.add(canonical)

    return title, meta_desc, text_sample, sorted(list(extracted_links))
