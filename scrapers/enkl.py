# scrapers/enkl.py
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from utils import canonicalize_url, generate_id

SOURCE = "enkl"

MONTHS_SV = {
    "januari": "01",
    "februari": "02",
    "mars": "03",
    "april": "04",
    "maj": "05",
    "juni": "06",
    "juli": "07",
    "augusti": "08",
    "september": "09",
    "oktober": "10",
    "november": "11",
    "december": "12",
}


def fetch(url: str) -> List[Dict[str, str]]:
    """
    ENKL Lediga uppdrag (HTML). Extracts list items + follows detail page for location.
    Returns list[dict]:
      {id,title,company,location,published,url}
    """
    start_url = url.strip()
    if not start_url:
        return []

    html = _get(start_url)
    soup = BeautifulSoup(html, "html.parser")

    # The page is WordPress-like; safest is to find all h3 links under the main content
    # and then look backwards for a nearby "Category + date" line.
    results: List[Dict[str, str]] = []
    seen: set[str] = set()

    for h3 in soup.find_all(["h3", "h2"]):
        a = h3.find("a", href=True)
        if not a:
            continue
        title = _clean(a.get_text(" ", strip=True))
        href = a["href"].strip()
        if not title or not href:
            continue

        canon = canonicalize_url(href)
        # Skip the generic “Söker du uppdrag som frilans?” pseudo-post (not a real assignment)
        if "/uppdrag/soker-du-uppdrag-som-frilans" in canon:
            continue

        # Find a nearby meta line (e.g. "IT 24 november 2025")
        category, published = _extract_meta_near(h3)
        published_iso = _sv_date_to_iso(published) if published else ""

        # Determine if this is already filled ("Tillsatt") by scanning nearby text
        nearby_text = _collect_nearby_text(h3)
        if re.search(r"\bTillsatt\b", nearby_text, re.IGNORECASE):
            # Usually not worth indexing, but if you want historical, remove this guard.
            continue

        # Detail page: pull location/Plats if present
        detail_html = _get(canon)
        location = _extract_location_from_detail(detail_html) or ""

        item = {
            "id": _stable_id(canon),
            "title": title,
            "company": "ENKL",
            "location": _clean(location),
            "published": published_iso or published or "",
            "url": canon,
        }

        if item["id"] in seen:
            continue
        seen.add(item["id"])
        results.append(item)

    return results


def _stable_id(canon_url: str) -> str:
    # ENKL doesn’t expose a nice numeric job-id reliably => namespace + canonical URL.
    return generate_id(f"{SOURCE}|{canon_url}")


def _get(url: str) -> str:
    r = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
        },
    )
    r.raise_for_status()
    return r.text


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _collect_nearby_text(tag) -> str:
    parts = []
    # Grab a small window around the title node for status words like "Tillsatt"
    for el in list(tag.previous_siblings)[-4:] + [tag] + list(tag.next_siblings)[:6]:
        try:
            txt = ""
            if hasattr(el, "get_text"):
                txt = el.get_text(" ", strip=True)
            else:
                txt = str(el).strip()
            if txt:
                parts.append(txt)
        except Exception:
            continue
    return " ".join(parts)


def _extract_meta_near(h_tag) -> Tuple[str, str]:
    """
    Attempts to parse "Category dd month yyyy" line near the title.
    Returns (category, date_str)
    """
    # Look a few siblings backward for a line containing a Swedish date.
    date_re = re.compile(r"\b(\d{1,2})\s+([a-zåäö]+)\s+(\d{4})\b", re.IGNORECASE)
    for el in list(h_tag.previous_siblings)[-8:]:
        if not hasattr(el, "get_text"):
            continue
        txt = _clean(el.get_text(" ", strip=True))
        if not txt:
            continue
        m = date_re.search(txt)
        if m:
            # Category often precedes date, e.g. "IT 24 november 2025"
            category = _clean(txt[: m.start()].strip(" -•|")) or ""
            date_str = _clean(m.group(0))
            return category, date_str
    return "", ""


def _sv_date_to_iso(date_str: str) -> str:
    """
    "20 februari 2026" -> "2026-02-20"
    If parsing fails, returns "".
    """
    m = re.search(r"\b(\d{1,2})\s+([a-zåäö]+)\s+(\d{4})\b", date_str.lower())
    if not m:
        return ""
    day = int(m.group(1))
    month_name = m.group(2)
    year = int(m.group(3))
    mm = MONTHS_SV.get(month_name)
    if not mm:
        return ""
    return f"{year:04d}-{mm}-{day:02d}"


def _extract_location_from_detail(detail_html: str) -> Optional[str]:
    """
    Extracts location from detail page text using robust regex.
    Looks for 'Plats:' or 'Location:' etc.
    """
    soup = BeautifulSoup(detail_html, "html.parser")
    text = _clean(soup.get_text(" ", strip=True))

    # Common patterns on ENKL detail pages
    patterns = [
        r"\bPlats:\s*([^\.|•]+?)(?:\s{2,}|Period:|Omfattning:|Start:|Språk:|Sista|$)",
        r"\bPlacering:\s*([^\.|•]+?)(?:\s{2,}|Period:|Omfattning:|Start:|Språk:|Sista|$)",
        r"\bLocation:\s*([^\.|•]+?)(?:\s{2,}|Period:|Extent:|Start:|Language:|Deadline|$)",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return _clean(m.group(1))
    return None