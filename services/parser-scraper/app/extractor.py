import hashlib
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

BOILERPLATE_TAGS = [
    "script", "style", "noscript", "svg", "header", "footer",
    "nav", "aside", "form", "iframe", "button"
]


def extract_clean_body_text(html: str) -> str:
    """
    Remove boilerplate HTML and normalize visible body text for fingerprinting.
    """
    soup = BeautifulSoup(html, "html.parser")
    for element in soup(BOILERPLATE_TAGS):
        element.decompose()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def body_fingerprint(text: str) -> int:
    """
    Compute a 64-bit integer fingerprint from normalized body text.
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def estimate_tokens(text: str) -> int:
    """
    Rule-of-thumb token estimator for LLMs (~4 characters per token).
    """
    return max(1, len(text) // 4) if text else 0


def html_to_markdown(html: str) -> str:
    """
    Transforms raw HTML into clean, token-efficient Markdown for LLMs & RAG pipelines.
    Strips menus, ads, footers, and scripts while preserving headings, links, code, and lists.
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1. Strip boilerplate elements
    for el in soup(BOILERPLATE_TAGS):
        el.decompose()

    # 2. Convert headings to Markdown
    for i in range(1, 7):
        for h in soup.find_all(f"h{i}"):
            h_text = h.get_text(" ", strip=True)
            if h_text:
                h.replace_with(f"\n\n{'#' * i} {h_text}\n\n")

    # 3. Convert code blocks
    for pre in soup.find_all("pre"):
        code_text = pre.get_text("\n", strip=True)
        pre.replace_with(f"\n\n```\n{code_text}\n```\n\n")

    for code in soup.find_all("code"):
        if code.parent and code.parent.name != "pre":
            inline_code = code.get_text(" ", strip=True)
            if inline_code:
                code.replace_with(f"`{inline_code}`")

    # 4. Convert links
    for a in soup.find_all("a", href=True):
        link_text = a.get_text(" ", strip=True)
        href = a["href"].strip()
        if link_text and href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
            a.replace_with(f"[{link_text}]({href})")

    # 5. Convert lists
    for li in soup.find_all("li"):
        li_text = li.get_text(" ", strip=True)
        if li_text:
            li.replace_with(f"\n- {li_text}")

    # 6. Convert blockquotes
    for bq in soup.find_all("blockquote"):
        bq_text = bq.get_text(" ", strip=True)
        if bq_text:
            bq.replace_with(f"\n\n> {bq_text}\n\n")

    # 7. Convert paragraphs
    for p in soup.find_all("p"):
        p_text = p.get_text(" ", strip=True)
        if p_text:
            p.replace_with(f"\n\n{p_text}\n\n")

    raw_md = soup.get_text()
    # Normalize multiple newlines and spaces
    clean_md = re.sub(r"\n{3,}", "\n\n", raw_md)
    clean_md = re.sub(r"[ \t]+", " ", clean_md)
    return clean_md.strip()


def canonicalize_url(base_url: str, raw_href: str, stay_in_domain: bool = False) -> str | None:
    """
    Normalizes a discovered link according to crawler standards:
    1. Resolves relative URLs to absolute.
    2. Strips URL fragments (#hash).
    3. Validates HTTP/HTTPS schemes.
    4. Filters out non-HTML assets (images, archives, media).
    5. Optionally restricts crawling strictly to the base domain/subdomain.
    """
    if not raw_href or raw_href.startswith(("javascript:", "mailto:", "tel:", "#")):
        return None

    # Resolve relative URL against base page URL
    absolute_url = urljoin(base_url, raw_href.strip())

    # Strip fragments (#section-1)
    defragged, _ = urldefrag(absolute_url)

    parsed = urlparse(defragged)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return None

    # Check file extension
    path_lower = parsed.path.lower()
    for ext in IGNORED_EXTENSIONS:
        if path_lower.endswith(ext):
            return None

    # Enforce Domain Guard if requested
    if stay_in_domain:
        base_host = (urlparse(base_url).hostname or "").lower()
        target_host = (parsed.hostname or "").lower()
        if not (target_host == base_host or target_host.endswith("." + base_host)):
            return None

    return defragged


def extract_content(
    html: str,
    base_url: str,
    stay_in_domain: bool = False
) -> Tuple[str, str, str, str, List[str]]:
    """
    Parses HTML DOM tree:
    - Extracts page <title>
    - Extracts <meta name="description">
    - Extracts clean body text sample (first 500 chars)
    - Converts body into clean, token-efficient LLM Markdown
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

    # 3. Clean Text Sample (first 500 chars)
    clean_sample_soup = BeautifulSoup(html, "html.parser")
    for element in clean_sample_soup(BOILERPLATE_TAGS):
        element.decompose()
    raw_text = clean_sample_soup.get_text(separator=" ", strip=True)
    text_sample = raw_text[:500] if raw_text else ""

    # 4. Generate LLM Markdown
    markdown = html_to_markdown(html)

    # 5. Extract Outbound Links
    extracted_links = set()
    for a_tag in soup.find_all("a", href=True):
        canonical = canonicalize_url(base_url, a_tag["href"], stay_in_domain=stay_in_domain)
        if canonical and canonical != base_url:
            extracted_links.add(canonical)

    return title, meta_desc, text_sample, markdown, sorted(list(extracted_links))
