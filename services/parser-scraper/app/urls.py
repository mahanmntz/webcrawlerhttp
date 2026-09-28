"""
Canonical URL form and crawl scope, shared with the other services.

Rules and test vectors live in shared/contracts/url_canonicalization.json;
api-gateway/src/url.ts (canonicalize) and crawler-engine/internal/scope (scope)
implement the same rules. Keep them in sync.
"""
from urllib.parse import urlsplit, urlunsplit

DEFAULT_PORTS = {"http": 80, "https": 443}


def _remove_dot_segments(path: str) -> str:
    """RFC 3986 section 5.2.4."""
    output: list[str] = []
    for segment in path.split("/")[1:]:
        if segment == "..":
            if output:
                output.pop()
        elif segment != ".":
            output.append(segment)
    # A trailing '.' or '..' still denotes a directory.
    if path.endswith(("/.", "/..")):
        output.append("")
    return "/" + "/".join(output)


def canonicalize(raw_url: str) -> str | None:
    try:
        parts = urlsplit(raw_url.strip())
        port = parts.port
        hostname = parts.hostname
    except ValueError:
        return None

    scheme = parts.scheme.lower()
    if scheme not in DEFAULT_PORTS or not hostname:
        return None

    try:
        host = hostname.encode("idna").decode("ascii")
    except UnicodeError:
        return None

    netloc = host if port in (None, DEFAULT_PORTS[scheme]) else f"{host}:{port}"
    path = _remove_dot_segments(parts.path) if parts.path else "/"

    return urlunsplit((scheme, netloc, path, parts.query, ""))


def _strip_www(host: str) -> str:
    host = host.lower().rstrip(".")
    return host[4:] if host.startswith("www.") else host


def in_scope(host: str, scope_host: str) -> bool:
    """True if host is scope_host or one of its subdomains, ignoring a leading 'www.'."""
    host, scope_host = _strip_www(host), _strip_www(scope_host)
    if not host or not scope_host:
        return False
    return host == scope_host or host.endswith("." + scope_host)
