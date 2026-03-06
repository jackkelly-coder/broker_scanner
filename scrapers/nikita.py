# scrapers/nikita.py

import logging
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from utils import clean_text

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nikita.se"
LIST_URL = "https://www.nikita.se/lediga-uppdrag/"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; broker-scanner/1.0)"}
MAX_PAGES = 50  # safety

_RX_DATE_PREFIX = re.compile(r"^\s*\d{1,2}\s+[A-Za-zÅÄÖåäö]{3}\s+", re.IGNORECASE)
_RX_PAGER_ONLY = re.compile(r"^\s*(\d+|»|«|›|‹)\s*$")
_RX_PAGER_PATH = re.compile(r"^lediga-uppdrag/(page|sida)/\d+/?$", re.IGNORECASE)


def _is_closed(title: str) -> bool:
    t = (title or "").strip().lower()
    return ("stängd" in t) or ("closed" in t)


def _strip_date_prefix(title: str) -> str:
    t = clean_text(title)
    return clean_text(_RX_DATE_PREFIX.sub("", t))


def _is_detail_url(full_url: str) -> bool:
    """
    Accept /lediga-uppdrag/<slug>/ but avoid listing and pagination pages.
    """
    if not full_url or "/lediga-uppdrag/" not in full_url:
        return False

    try:
        path = urlparse(full_url).path or ""
    except Exception:
        path = full_url

    p = path.strip("/")

    # listing root
    if p == "lediga-uppdrag":
        return False

    # pagination: /lediga-uppdrag/page/2/
    if _RX_PAGER_PATH.match(p):
        return False

    # Must have at least one segment after lediga-uppdrag
    return p.startswith("lediga-uppdrag/") and len(p.split("/")) >= 2


def _extract_location_from_context(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"(Ort|Plats)\s*:\s*([A-Za-zÅÄÖåäö\s\-]+)", text, re.IGNORECASE)
    if m:
        return clean_text(m.group(2))
    return ""


def _find_next_page(soup: BeautifulSoup) -> str:
    a = soup.select_one("a.next.page-numbers") or soup.select_one("a.next")
    if not a:
        return ""
    href = a.get("href") or ""
    if not href:
        return ""
    return href if href.startswith("http") else urljoin(BASE_URL, href)


def fetch(url: str):
    list_url = url or LIST_URL

    results = []
    seen_urls = set()
    page_url = list_url
    page_no = 0

    while page_url and page_no < MAX_PAGES:
        page_no += 1

        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Nikita fetch failed (page %s): %s", page_no, e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.select('a[href*="/lediga-uppdrag/"]'):
            href = a.get("href") or ""
            full_url = href if href.startswith("http") else urljoin(BASE_URL, href)

            if not _is_detail_url(full_url):
                continue

            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            raw = clean_text(a.get_text(" ", strip=True))
            if not raw:
                continue

            # Drop pager anchors: "2", "10", "»", ...
            if _RX_PAGER_ONLY.match(raw):
                continue

            title = _strip_date_prefix(raw)
            if not title:
                continue
            if _is_closed(title):
                continue

            parent = a.find_parent()
            context_text = clean_text(parent.get_text(" ", strip=True)) if parent else ""
            location = _extract_location_from_context(context_text) or "Sweden"

            results.append({
                "id": None,
                "title": title,
                "company": "Nikita",
                "location": location,
                "published": "",
                "url": full_url,
            })

        next_url = _find_next_page(soup)
        if not next_url or next_url == page_url:
            break
        page_url = next_url

    logger.info("Nikita scraped %s items (open only)", len(results))
    return results