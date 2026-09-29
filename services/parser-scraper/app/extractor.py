import hashlib
import json
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
    "nav", "aside", "form", "iframe", "button", "dialog", "template",
    "canvas", "audio", "video", "select", "option", "textarea"
]

BOILERPLATE_CLASSES_OR_IDS = re.compile(
    r"(cookie|banner|consent|modal|popup|advert|ad-|ads-|social-share|sidebar|promo|newsletter|"
    r"reflist|references|citation|footnote|mw-jump-link|mw-editsection|navbox|vertical-navbox|"
    r"catlinks|printfooter|\b(toc|table-of-contents|vector-toc|vector-menu|hatnote|infobox|ambox|"
    r"mw-empty-elt|noprint|hidden-print|sr-only|visually-hidden|screen-reader-text)\b)",
    re.I
)


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


def _strip_citations_and_references(root) -> None:
    """
    Strips Wikipedia / academic footnote references like [1], [2], [citation needed].
    """
    for sup in list(root.find_all("sup")):
        if sup.name in ("html", "body"):
            continue
        text = sup.get_text(strip=True)
        if re.match(r"^\[?\d+\]?$", text) or "citation" in text.lower():
            sup.decompose()
    for ref in list(root.find_all(class_=re.compile(r"^(reference|citation|mw-ref)$", re.I))):
        if ref.name in ("html", "body"):
            continue
        ref.decompose()


def _strip_hidden_elements(soup: BeautifulSoup) -> None:
    """
    Strips visually hidden elements (aria-hidden, display:none, visibility:hidden, hidden attr).
    """
    for el in list(soup.find_all(attrs={"hidden": True})):
        if el.name in ("html", "body"):
            continue
        el.decompose()
    for el in list(soup.find_all(attrs={"aria-hidden": "true"})):
        if el.name in ("html", "body"):
            continue
        el.decompose()
    for el in list(soup.find_all(style=re.compile(r"(display\s*:\s*none|visibility\s*:\s*hidden)", re.I))):
        if el.name in ("html", "body"):
            continue
        el.decompose()


def _strip_high_link_density(root, threshold: float = 0.55) -> None:
    """
    Trafilatura-style heuristic: blocks with very high link density (>55%)
    and multiple links are navigation bars, link clouds, or lists of external links.
    """
    for el in list(root.find_all(["div", "section", "ul", "ol", "table"])):
        if el.name in ("html", "body") or not el.parent:
            continue
        # Never strip the main container itself
        if el.get("id") in ("content", "main-content", "mw-content-text", "bodyContent") or \
           any(c in ("article-content", "post-content", "entry-content", "markdown-body") for c in (el.get("class") or [])):
            continue
        text = el.get_text(strip=True)
        if len(text) < 40:
            continue
        links = el.find_all("a")
        if len(links) >= 3:
            link_text = "".join(a.get_text(strip=True) for a in links)
            density = len(link_text) / len(text)
            if density >= threshold:
                el.decompose()


def _convert_tables_to_markdown(root) -> None:
    """
    Converts data tables to compact GFM Markdown tables; layout/nav tables are removed.
    """
    for table in list(root.find_all("table")):
        if table.name in ("html", "body"):
            continue
        rows = table.find_all("tr")
        if not rows:
            table.decompose()
            continue

        md_rows = []
        header_cols = 0
        for idx, tr in enumerate(rows):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            cell_texts = [re.sub(r"\s+", " ", c.get_text(" ", strip=True)).replace("|", "\\|") for c in cells]
            if not any(cell_texts):
                continue
            if idx == 0:
                header_cols = len(cell_texts)
                md_rows.append("| " + " | ".join(cell_texts) + " |")
                md_rows.append("| " + " | ".join(["---"] * header_cols) + " |")
            else:
                if header_cols > 0:
                    if len(cell_texts) < header_cols:
                        cell_texts.extend([""] * (header_cols - len(cell_texts)))
                    elif len(cell_texts) > header_cols:
                        cell_texts = cell_texts[:header_cols]
                md_rows.append("| " + " | ".join(cell_texts) + " |")

        if md_rows:
            table.replace_with(f"\n\n" + "\n".join(md_rows) + "\n\n")
        else:
            table.decompose()


def _get_main_content_root(soup: BeautifulSoup):
    """
    Identifies the semantic primary article container if one exists,
    stripping page margins, sidebars, and peripheral noise.
    """
    candidates = [
        soup.find("article"),
        soup.find("main"),
        soup.find(attrs={"role": "main"}),
        soup.find(id=re.compile(r"^(content|main-content|mw-content-text|bodyContent|article-body|post-content)$", re.I)),
        soup.find(class_=re.compile(r"^(article-content|post-content|entry-content|markdown-body|content-body)$", re.I)),
    ]
    for c in candidates:
        if c and len(c.get_text(strip=True)) > 80:
            return c
    return soup.body or soup


def _strip_boilerplate(soup: BeautifulSoup) -> None:
    for element in soup(BOILERPLATE_TAGS):
        if element.name in ("html", "body"):
            continue
        element.decompose()
    _strip_hidden_elements(soup)
    # Strip elements with ad/cookie/banner class or id to save tokens
    for element in soup.find_all(attrs={"class": BOILERPLATE_CLASSES_OR_IDS}):
        if element.name in ("html", "body"):
            continue
        element.decompose()
    for element in soup.find_all(attrs={"id": BOILERPLATE_CLASSES_OR_IDS}):
        if element.name in ("html", "body"):
            continue
        element.decompose()
    _strip_citations_and_references(soup)
    _strip_high_link_density(soup)


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
    root = _get_main_content_root(soup)

    # 1. Convert tables to compact GFM tables
    _convert_tables_to_markdown(root)

    # 2. Convert headings to Markdown
    for i in range(1, 7):
        for h in root.find_all(f"h{i}"):
            h_text = h.get_text(" ", strip=True)
            if h_text:
                h.replace_with(f"\n\n{'#' * i} {h_text}\n\n")

    # 3. Convert code blocks
    for pre in root.find_all("pre"):
        code_text = pre.get_text("\n", strip=True)
        pre.replace_with(f"\n\n```\n{code_text}\n```\n\n")

    for code in root.find_all("code"):
        if code.parent and code.parent.name != "pre":
            inline_code = code.get_text(" ", strip=True)
            if inline_code:
                code.replace_with(f"`{inline_code}`")

    # 4. Convert links (Token-saver: keep external absolute links, strip redundant internal URL syntax)
    for a in root.find_all("a", href=True):
        link_text = a.get_text(" ", strip=True)
        href = a["href"].strip()
        if not link_text or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            a.replace_with(link_text)
            continue
        # If relative or in-domain internal anchor link (/wiki/..., /doc/...), keep anchor text only to save tokens
        if href.startswith("/") or not href.startswith(("http://", "https://")):
            a.replace_with(link_text)
        else:
            a.replace_with(f"[{link_text}]({href})")

    # 5. Convert lists
    for li in root.find_all("li"):
        li_text = li.get_text(" ", strip=True)
        if li_text:
            li.replace_with(f"\n- {li_text}")

    # 6. Convert blockquotes
    for bq in root.find_all("blockquote"):
        bq_text = bq.get_text(" ", strip=True)
        if bq_text:
            bq.replace_with(f"\n\n> {bq_text}\n\n")

    # 7. Convert paragraphs
    for p in root.find_all("p"):
        p_text = p.get_text(" ", strip=True)
        if p_text:
            p.replace_with(f"\n\n{p_text}\n\n")

    raw_md = root.get_text()

    # Post-process for token optimization:
    # 1. Remove empty list items
    raw_md = re.sub(r"^\s*[-*]\s*$", "", raw_md, flags=re.MULTILINE)
    # 2. Normalize whitespace runs
    raw_md = re.sub(r"[ \t]+", " ", raw_md)
    # 3. Remove spaces before punctuation (e.g., 'word .' -> 'word.')
    raw_md = re.sub(r"\s+([,.:;?!])", r"\1", raw_md)
    # 4. Collapse multiple newlines into standard double newlines
    clean_md = re.sub(r"\n{3,}", "\n\n", raw_md)
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
    language: str = "en"
    metadata: dict = field(default_factory=dict)


def detect_language(soup: BeautifulSoup, sample_text: str = "") -> str:
    """
    Detects language from html lang attribute, meta headers, or content script cues.
    Returns 2-letter ISO code or 'unknown' (e.g. 'fa', 'en', 'es', 'de').
    """
    # 1. <html lang="...">
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        lang = html_tag["lang"].strip().lower()
        return lang.split("-")[0].split("_")[0]

    # 2. <meta http-equiv="content-language" content="...">
    meta_lang = soup.find("meta", attrs={"http-equiv": re.compile(r"content-language", re.I)})
    if meta_lang and meta_lang.get("content"):
        lang = meta_lang["content"].strip().lower()
        return lang.split("-")[0].split(",")[0].strip()

    # 3. Script heuristics for Persian / Arabic vs Latin
    if sample_text:
        persian_arabic_chars = len(re.findall(r"[\u0600-\u06FF\uFB8A\u067E\u0686\u06AF]", sample_text))
        latin_chars = len(re.findall(r"[a-zA-Z]", sample_text))
        cyrillic_chars = len(re.findall(r"[\u0400-\u04FF]", sample_text))
        total = max(1, persian_arabic_chars + latin_chars + cyrillic_chars)

        if persian_arabic_chars / total > 0.4:
            return "fa"
        elif cyrillic_chars / total > 0.4:
            return "ru"
        elif latin_chars / total > 0.4:
            return "en"

    return "en"


def extract_rich_metadata(soup: BeautifulSoup, url: str) -> dict:
    """
    Extracts OpenGraph, Twitter Card, and Schema.org JSON-LD metadata for LLM grounding.
    """
    meta: dict = {}

    # OpenGraph & Twitter tags
    for tag in soup.find_all("meta"):
        prop = tag.get("property") or tag.get("name") or ""
        prop = prop.strip().lower()
        content = tag.get("content", "").strip()
        if not prop or not content:
            continue

        if prop in ("og:title", "twitter:title"):
            meta.setdefault("og_title", content)
        elif prop in ("og:description", "twitter:description"):
            meta.setdefault("og_description", content)
        elif prop in ("og:image", "twitter:image"):
            meta.setdefault("image", content)
        elif prop in ("article:author", "author"):
            meta.setdefault("author", content)
        elif prop in ("article:published_time", "date", "pubdate"):
            meta.setdefault("published_time", content)
        elif prop in ("og:site_name", "application-name"):
            meta.setdefault("site_name", content)

    # Schema.org JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        if script.string:
            try:
                data = json.loads(script.string.strip())
                if isinstance(data, list) and data:
                    data = data[0]
                if isinstance(data, dict):
                    if "@type" in data:
                        meta.setdefault("schema_type", str(data.get("@type")))
                    if "headline" in data:
                        meta.setdefault("headline", str(data.get("headline")))
                    if "author" in data:
                        author = data["author"]
                        if isinstance(author, dict) and "name" in author:
                            meta.setdefault("author", str(author["name"]))
                        elif isinstance(author, str):
                            meta.setdefault("author", author)
                    if "datePublished" in data:
                        meta.setdefault("published_time", str(data.get("datePublished")))
            except Exception:
                pass

    return meta


def chunk_markdown(
    markdown: str,
    doc_url: str = "",
    doc_title: str = "",
    max_tokens: int = 400,
    overlap_tokens: int = 50
) -> List[dict]:
    """
    Splits clean Markdown into semantic, heading-aware chunks optimized for Vector DBs.
    Each chunk retains its parent section heading, metadata, and token count.
    """
    if not markdown or not markdown.strip():
        return []

    lines = markdown.split("\n")
    sections = []
    current_heading = doc_title or "Overview"
    current_lines = []

    for line in lines:
        if line.startswith("#"):
            if current_lines:
                sec_text = "\n".join(current_lines).strip()
                if sec_text:
                    sections.append((current_heading, sec_text))
                current_lines = []
            current_heading = line.lstrip("#").strip()
        else:
            current_lines.append(line)

    if current_lines:
        sec_text = "\n".join(current_lines).strip()
        if sec_text:
            sections.append((current_heading, sec_text))

    chunks = []
    chunk_index = 0

    for heading, text in sections:
        words = text.split()
        if not words:
            continue

        # ~1 token per word heuristic (or max_tokens limit)
        step = max(20, max_tokens - overlap_tokens)
        for i in range(0, len(words), step):
            chunk_words = words[i:i + max_tokens]
            chunk_content = " ".join(chunk_words)
            chunk_tokens = estimate_tokens(chunk_content)
            chunk_index += 1

            chunks.append({
                "chunk_id": f"{hashlib.sha256((doc_url + str(chunk_index)).encode('utf-8')).hexdigest()[:12]}",
                "index": chunk_index,
                "url": doc_url,
                "title": doc_title,
                "section": heading,
                "content": f"### {heading}\n\n{chunk_content}",
                "estimated_tokens": chunk_tokens,
            })

    return chunks


def extract_page(
    html: str,
    base_url: str,
    stay_in_domain: bool = False,
    scope_host: str | None = None,
) -> PageExtraction:
    """
    Parses the HTML once and derives everything the pipeline needs:
    title, meta description, outbound links, text sample, fingerprint text,
    language, rich metadata, and LLM Markdown.
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

    # 3. Extract Rich Metadata (OpenGraph / JSON-LD) before modifying soup
    metadata = extract_rich_metadata(soup, base_url)

    # 4. Extract Outbound Links (before stripping nav/footer)
    own_url = canonicalize(base_url)
    links = set()
    for a_tag in soup.find_all("a", href=True):
        canonical = canonicalize_url(base_url, a_tag["href"], stay_in_domain=stay_in_domain, scope_host=scope_host)
        if canonical and canonical != own_url:
            links.add(canonical)

    # 5. Clean Text Sample & fingerprint text
    _strip_boilerplate(soup)
    raw_text = _body_text(soup)
    language = detect_language(soup, raw_text[:500])

    return PageExtraction(
        title=title,
        meta_description=meta_desc,
        text_sample=raw_text[:500],
        markdown=_soup_to_markdown(soup),
        links=sorted(links),
        fingerprint_text=_normalize_for_fingerprint(raw_text),
        language=language,
        metadata=metadata,
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
