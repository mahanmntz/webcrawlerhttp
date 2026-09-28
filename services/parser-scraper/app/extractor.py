import hashlib
import re
from dataclasses import dataclass, field
from typing import List, Tuple
from urllib.parse import urljoin, urlsplit
from bs4 import BeautifulSoup

from app.urls import canonicalize, in_scope

IGNORED_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".pdf", ".zip", ".tar", ".gz", ".rar", ".7z",
    ".mp3", ".mp4", ".avi", ".mov", ".wav",
    ".css", ".js", ".json", ".xml", ".ico", ".woff", ".woff2"
)

BOILERPLATE_TAGS = [
    "script", "style", "noscript", "svg", "header", "footer",
    "nav", "aside", "form", "iframe", "button"
]


def extract_clean_body_text(html: str) -> str:
    """
    Remove boilerplate HTML and normalize visible body text for fingerprinting.
    """
    soup = BeautifulSoup(html, "html.parser")
    _strip_boilerplate(soup)
    return _normalize_for_fingerprint(_body_text(soup))


def _body_text(soup: BeautifulSoup) -> str:
    # <body> only: <title> differs between mirrors and would defeat content dedup.
    return (soup.body or soup).get_text(" ", strip=True)


def _strip_boilerplate(soup: BeautifulSoup) -> None:
    for element in soup(BOILERPLATE_TAGS):
        element.decompose()


def _normalize_for_fingerprint(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


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
    _strip_boilerplate(soup)
    return _soup_to_markdown(soup)


def _soup_to_markdown(soup: BeautifulSoup) -> str:
    """Converts an already boilerplate-free soup to Markdown. Mutates the soup."""
    # 1. Convert headings to Markdown
    for i in range(1, 7):
        for h in soup.find_all(f"h{i}"):
            h_text = h.get_text(" ", strip=True)
            if h_text:
                h.replace_with(f"\n\n{'#' * i} {h_text}\n\n")

    # 2. Convert code blocks
    for pre in soup.find_all("pre"):
        code_text = pre.get_text("\n", strip=True)
        pre.replace_with(f"\n\n```\n{code_text}\n```\n\n")

    for code in soup.find_all("code"):
        if code.parent and code.parent.name != "pre":
            inline_code = code.get_text(" ", strip=True)
            if inline_code:
                code.replace_with(f"`{inline_code}`")

    # 3. Convert links
    for a in soup.find_all("a", href=True):
        link_text = a.get_text(" ", strip=True)
        href = a["href"].strip()
        if link_text and href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
            a.replace_with(f"[{link_text}]({href})")

    # 4. Convert lists
    for li in soup.find_all("li"):
        li_text = li.get_text(" ", strip=True)
        if li_text:
            li.replace_with(f"\n- {li_text}")

    # 5. Convert blockquotes
    for bq in soup.find_all("blockquote"):
        bq_text = bq.get_text(" ", strip=True)
        if bq_text:
            bq.replace_with(f"\n\n> {bq_text}\n\n")

    # 6. Convert paragraphs
    for p in soup.find_all("p"):
        p_text = p.get_text(" ", strip=True)
        if p_text:
            p.replace_with(f"\n\n{p_text}\n\n")

    raw_md = soup.get_text()
    # Normalize multiple newlines and spaces
    clean_md = re.sub(r"\n{3,}", "\n\n", raw_md)
    clean_md = re.sub(r"[ \t]+", " ", clean_md)
    return clean_md.strip()


def canonicalize_url(
    base_url: str,
    raw_href: str,
    stay_in_domain: bool = False,
    scope_host: str | None = None,
) -> str | None:
    """
    Normalizes a discovered link according to crawler standards:
    1. Resolves relative URLs to absolute.
    2. Applies the shared canonical form (see app/urls.py).
    3. Filters out non-HTML assets (images, archives, media).
    4. Optionally restricts crawling to scope_host (the seed's host) and its
       subdomains. Without a scope_host, the base page's host is used.
    """
    if not raw_href or raw_href.strip().startswith(("javascript:", "mailto:", "tel:", "#")):
        return None

    canonical = canonicalize(urljoin(base_url, raw_href.strip()))
    if canonical is None:
        return None

    parsed = urlsplit(canonical)
    if parsed.path.lower().endswith(IGNORED_EXTENSIONS):
        return None

    if stay_in_domain:
        boundary = scope_host or urlsplit(base_url).hostname or ""
        if not in_scope(parsed.hostname or "", boundary):
            return None

    return canonical


@dataclass
class PageExtraction:
    title: str = ""
    meta_description: str = ""
    text_sample: str = ""
    markdown: str = ""
    links: List[str] = field(default_factory=list)
    # Normalized boilerplate-free body text; empty for pages with no text.
    fingerprint_text: str = ""


def extract_page(
    html: str,
    base_url: str,
    stay_in_domain: bool = False,
    scope_host: str | None = None,
) -> PageExtraction:
    """
    Parses the HTML once and derives everything the pipeline needs:
    title, meta description, outbound links, text sample, fingerprint text
    and LLM Markdown.
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

    # 3. Extract Outbound Links (before stripping nav/footer: they are the
    #    main source of discovery)
    own_url = canonicalize(base_url)
    links = set()
    for a_tag in soup.find_all("a", href=True):
        canonical = canonicalize_url(base_url, a_tag["href"], stay_in_domain=stay_in_domain, scope_host=scope_host)
        if canonical and canonical != own_url:
            links.add(canonical)

    # 4. Clean Text Sample & fingerprint text
    _strip_boilerplate(soup)
    raw_text = _body_text(soup)

    return PageExtraction(
        title=title,
        meta_description=meta_desc,
        text_sample=raw_text[:500],
        # 5. Generate LLM Markdown (mutates the soup, so it goes last)
        markdown=_soup_to_markdown(soup),
        links=sorted(links),
        fingerprint_text=_normalize_for_fingerprint(raw_text),
    )


def extract_content(
    html: str,
    base_url: str,
    stay_in_domain: bool = False,
    scope_host: str | None = None,
) -> Tuple[str, str, str, str, List[str]]:
    """Tuple form of extract_page: (title, meta, text_sample, markdown, links)."""
    page = extract_page(html, base_url, stay_in_domain=stay_in_domain, scope_host=scope_host)
    return page.title, page.meta_description, page.text_sample, page.markdown, page.links
