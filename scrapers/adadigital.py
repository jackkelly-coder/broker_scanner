import os
import re
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from utils import clean_text, normalize_url, canonicalize_url, generate_id


HEADERS = {
    "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
    "User-Agent": "Mozilla/5.0 (compatible; broker_scanner/1.0)",
}
TIMEOUT_S = 25

PUBLISHED_RX = re.compile(r"Publicerat\s+(\d{4}-\d{2}-\d{2})", re.IGNORECASE)
REMOTE_HINTS = ["remote", "distans", "på distans", "pa distans", "hybrid", "wfh", "work from home"]


def _path_parts(href: str) -> list[str]:
    try:
        p = urlparse(href)
    except Exception:
        return []
    path = (p.path or "").strip()
    return [x for x in path.split("/") if x]


def _is_job_detail_href(href: str) -> bool:
    """
    Accept only real job detail pages.

    Real job:
      /lediga-jobb/<kategori>/<slug>[/]
      -> exactly 3 path parts

    Exclude filters:
      /lediga-jobb/ort/<stad>[/]
      /lediga-jobb/yrke/<kategori>[/]
      /lediga-jobb (landing)
    """
    parts = _path_parts(href)
    if len(parts) != 3:
        return False

    if parts[0] != "lediga-jobb":
        return False

    # hard exclude known filter namespaces
    if parts[1] in {"ort", "yrke"}:
        return False

    # also avoid weird slugs that are actually filter hubs
    if parts[2] in {"ort", "yrke"}:
        return False

    return True


def _looks_like_country_or_code(s: str) -> bool:
    low = (s or "").lower()
    return any(x in low for x in ["sweden", "sverige", " se", ",se", "swe"])


def _ensure_sweden_location(location: str) -> str:
    """
    Ada Digital är en Sverige-site. Om location bara är en svensk ort (t.ex. Tyresö),
    så behöver vi hjälpa Sweden-filter genom att lägga till ', Sweden'.
    """
    loc = clean_text(location)
    if not loc:
        return "Sweden"

    if _looks_like_country_or_code(loc):
        return loc

    # Already has explicit country separator?
    if "," in loc:
        return loc

    return f"{loc}, Sweden"


def _extract_location_near_link(job_a) -> str:
    """
    On the list page, the location often appears as a link:
      <a href="/lediga-jobb/...">Title</a>
      <a href="/lediga-jobb/ort/stockholm/">Stockholm</a>
    """
    cur = job_a
    for _ in range(0, 25):
        nxt = cur.find_next("a", href=True)
        if not nxt:
            break

        href = (nxt.get("href") or "").strip()

        # stop if we hit next job link
        if _is_job_detail_href(href):
            break

        # location link
        parts = _path_parts(href)
        if len(parts) >= 3 and parts[0] == "lediga-jobb" and parts[1] == "ort":
            return clean_text(nxt.get_text(" ", strip=True))

        cur = nxt

    return ""


def _detail_parse_location_and_published(html: str) -> tuple[str, str, bool]:
    soup = BeautifulSoup(html, "html.parser")
    full_text = clean_text(soup.get_text(" ", strip=True))
    low = full_text.lower()

    # Location: first /lediga-jobb/ort/<...> link
    loc = ""
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        parts = _path_parts(href)
        if len(parts) >= 3 and parts[0] == "lediga-jobb" and parts[1] == "ort":
            loc = clean_text(a.get_text(" ", strip=True))
            if loc:
                break

    # Published
    published = ""
    m = PUBLISHED_RX.search(full_text)
    if m:
        published = m.group(1)

    is_remote = any(h in low for h in REMOTE_HINTS)
    return (loc, published, is_remote)


def _fetch_detail_enrichment(session: requests.Session, url: str) -> tuple[str, str]:
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT_S)
        r.raise_for_status()
    except Exception:
        return ("", "")

    loc, published, is_remote = _detail_parse_location_and_published(r.text)

    if not loc and is_remote:
        loc = "Sweden (Remote)"

    # last fallback
    if not loc:
        loc = "Sweden"

    return (_ensure_sweden_location(loc), published)


def fetch(url: str):
    results = []
    sess = requests.Session()
    sess.headers.update(HEADERS)

    try:
        r = sess.get(url, timeout=TIMEOUT_S)
        r.raise_for_status()
    except Exception as e:
        print(f"[adadigital] Error fetching {url}: {e}")
        return results

    soup = BeautifulSoup(r.text, "html.parser")

    seen = set()

    # Collect only valid job detail links
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not _is_job_detail_href(href):
            continue

        title = clean_text(a.get_text(" ", strip=True))
        if not title or len(title) < 3:
            continue

        full_url = canonicalize_url(normalize_url(url, href))
        if not full_url or full_url in seen:
            continue
        seen.add(full_url)

        location = _extract_location_near_link(a)
        published = ""

        # Ensure Sweden-like location for Sweden filter
        location = _ensure_sweden_location(location) if location else "Sweden"

        # Always enrich published if missing (cheap at this scale: ~18 pages)
        if not published:
            det_loc, det_pub = _fetch_detail_enrichment(sess, full_url)
            if det_loc:
                location = det_loc
            if det_pub:
                published = det_pub

        if os.getenv("ADADIGITAL_DEBUG") == "1":
            print(f"[adadigital] title={title!r} location={location!r} published={published!r} url={full_url}")

        results.append(
            {
                "id": f"adadigital-{generate_id(full_url)}",
                "title": title,
                "company": "Ada Digital",
                "location": location,
                "published": published,
                "url": full_url,
            }
        )

    return results