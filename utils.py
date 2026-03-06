# utils.py
import hashlib
import re
from urllib.parse import urljoin, urlunparse, parse_qsl, urlencode, urlparse

_CONTROL_CHARS = re.compile(r"[\x00-\x1F\x7F-\x9F]")


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = _CONTROL_CHARS.sub("", s)
    s = " ".join(s.split())
    return s.strip()


def normalize_url(base_url: str, href: str) -> str:
    if not href:
        return ""
    href = clean_text(href)
    full = urljoin(base_url, href)
    return clean_text(full)


def generate_id(url: str) -> str:
    url = clean_text(url)
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def canonicalize_url(url: str) -> str:
    """
    Remove common tracking params and normalize URL for stable IDs.
    - Lowercases scheme+host
    - Drops fragments
    - Removes common tracking query params
    - Normalizes trailing slash (except root)
    - Removes default ports (:80/:443)
    """
    if not url:
        return ""

    u = urlparse(url.strip())

    scheme = (u.scheme or "https").lower()
    netloc = (u.netloc or "").lower()

    # Remove default ports
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    fragment = ""

    drop_prefixes = ("utm_",)
    drop_keys = {
        "gclid", "fbclid", "msclkid", "icid",
        "ref", "referrer", "source", "campaign", "medium",
    }

    q = []
    for k, v in parse_qsl(u.query, keep_blank_values=True):
        lk = k.lower()
        if any(lk.startswith(p) for p in drop_prefixes):
            continue
        if lk in drop_keys:
            continue
        q.append((k, v))

    query = urlencode(q)

    path = u.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunparse((scheme, netloc, path, u.params, query, fragment))