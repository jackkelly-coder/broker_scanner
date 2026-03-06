# scrapers/wiseit.py

import logging
import os
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from utils import clean_text

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; broker-scanner/1.0)"}

# Use tuple timeout: (connect, read)
CONNECT_TIMEOUT_S = float(os.getenv("WISEIT_CONNECT_TIMEOUT", "5"))
READ_TIMEOUT_S = float(os.getenv("WISEIT_READ_TIMEOUT", "20"))

WISEIT_MAX_JOBS = int(os.getenv("WISEIT_MAX_JOBS", "80"))
WISEIT_MAX_DETAILS = int(os.getenv("WISEIT_MAX_DETAILS", "30"))
WISEIT_DEBUG = os.getenv("WISEIT_DEBUG", "0") == "1"

JOB_PATH_RX = re.compile(r"^/jobb/[^/]+-\d+/?$", re.IGNORECASE)

CITY_HINTS = [
    ("stockholm", "Stockholm"),
    ("solna", "Stockholm"),
    ("sundbyberg", "Stockholm"),
    ("kista", "Stockholm"),
    ("bromma", "Stockholm"),
    ("inom tullarna", "Stockholm"),
    ("göteborg", "Göteborg"),
    ("goteborg", "Göteborg"),
    ("gothenburg", "Göteborg"),
    ("malmö", "Malmö"),
    ("malmo", "Malmö"),
    ("lund", "Malmö"),
    ("helsingborg", "Malmö"),
    ("uppsala", "Uppsala"),
    ("västerås", "Västerås"),
    ("vasteras", "Västerås"),
    ("örebro", "Örebro"),
    ("orebro", "Örebro"),
    ("linköping", "Linköping"),
    ("linkoping", "Linköping"),
    ("norrköping", "Norrköping"),
    ("norrkoping", "Norrköping"),
    ("jönköping", "Jönköping"),
    ("jonkoping", "Jönköping"),
    ("umeå", "Umeå"),
    ("umea", "Umeå"),
    ("sundsvall", "Sundsvall"),
    ("borås", "Borås"),
    ("boras", "Borås"),
    ("gävle", "Gävle"),
    ("gavle", "Gävle"),
    ("visby", "Visby"),
    ("kalmar", "Kalmar"),
    ("skövde", "Skövde"),
    ("skovde", "Skövde"),
    ("älmhult", "Älmhult"),
    ("almhult", "Älmhult"),
    ("ängelholm", "Ängelholm"),
    ("angelholm", "Ängelholm"),
]

REMOTE_HINTS = ["remote", "distans", "på distans", "pa distans", "hybrid", "wfh", "work from home"]


def _is_job_url(href: str) -> bool:
    if not href:
        return False
    try:
        path = urlparse(href).path
    except Exception:
        return False
    return bool(JOB_PATH_RX.match(path))


def _extract_title_from_card(a_tag) -> str:
    if not a_tag:
        return ""

    txt = clean_text(a_tag.get_text(" ", strip=True))
    if 3 <= len(txt) <= 140:
        return txt

    card = a_tag.find_parent(["article", "li", "div"])
    if card:
        for tag in ["h1", "h2", "h3", "h4"]:
            h = card.find(tag)
            if h:
                t = clean_text(h.get_text(" ", strip=True))
                if 3 <= len(t) <= 140:
                    return t

    return ""


def _extract_location_from_card(a_tag) -> str:
    if not a_tag:
        return ""

    card = a_tag.find_parent(["article", "li", "div"])
    if not card:
        return ""

    text = clean_text(card.get_text(" ", strip=True))
    low = text.lower()

    for needle, canonical in CITY_HINTS:
        if needle in low:
            return canonical

    if any(x in low for x in REMOTE_HINTS):
        return "Sweden (Remote)"

    return ""


def _fetch_detail_title(sess: requests.Session, url: str) -> str:
    try:
        r = sess.get(url, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S))
        r.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(r.text, "html.parser")

    h1 = soup.find("h1")
    if h1:
        t = clean_text(h1.get_text(" ", strip=True))
        if 3 <= len(t) <= 160:
            return t

    if soup.title:
        t = clean_text(soup.title.get_text(strip=True))
        if " - " in t:
            t = t.split(" - ")[0].strip()
        if 3 <= len(t) <= 160:
            return t

    return ""


def fetch(url: str):
    list_url = url
    sess = requests.Session()
    sess.headers.update(HEADERS)

    try:
        r = sess.get(list_url, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S))
        r.raise_for_status()
    except Exception as e:
        logger.warning("[wiseit] list fetch failed: %s | %s", list_url, e)
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    anchors = soup.select('a[href*="/jobb/"]')
    seen = set()
    results = []

    base = f"{urlparse(list_url).scheme}://{urlparse(list_url).netloc}"

    detail_tries = 0
    candidates = 0

    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href:
            continue

        full = href if href.startswith("http") else urljoin(base, href)
        if not _is_job_url(full):
            continue
        if full in seen:
            continue
        seen.add(full)

        candidates += 1
        if WISEIT_MAX_JOBS and len(results) >= WISEIT_MAX_JOBS:
            break

        title = _extract_title_from_card(a)

        if not title:
            if WISEIT_MAX_DETAILS and detail_tries >= WISEIT_MAX_DETAILS:
                # stop doing expensive detail lookups
                continue
            detail_tries += 1
            if detail_tries % 10 == 0:
                logger.info("[wiseit] detail fetch progress: %s/%s", detail_tries, WISEIT_MAX_DETAILS)
            title = _fetch_detail_title(sess, full)

        if not title:
            if WISEIT_DEBUG:
                logger.info("[wiseit] drop (no title) url=%s", full)
            continue

        location = _extract_location_from_card(a) or "Sweden"

        if WISEIT_DEBUG:
            logger.info("[wiseit] candidate title=%r location=%r url=%s", title, location, full)

        results.append(
            {
                "id": None,
                "title": title,
                "company": "Wise IT",
                "location": location,
                "published": "",
                "url": full,
            }
        )

    logger.info("[wiseit] scraped %s items (candidates=%s details=%s)", len(results), candidates, detail_tries)
    return results