# scrapers/qbemanning.py
import json
import logging
import os
import re
import time
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

from utils import clean_text

logger = logging.getLogger(__name__)

DEFAULT_LIST_URL = "https://careers.qbemanning.se/jobs"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; broker-scanner/1.0)"}

QB_SLEEP = float(os.getenv("QB_SLEEP", "0.02"))
QB_MAX_PAGES = int(os.getenv("QB_MAX_PAGES", "80"))
QB_DETAIL_TIMEOUT = int(os.getenv("QB_DETAIL_TIMEOUT", "20"))
QB_LIST_TIMEOUT = int(os.getenv("QB_LIST_TIMEOUT", "20"))

JOB_PATH_RX = re.compile(r"/jobs/\d+-", re.IGNORECASE)

COOKIE_TITLE_RX = re.compile(r"(välj vilka cookies|choose which cookies|cookies)", re.IGNORECASE)


def _is_closed(title: str) -> bool:
    t = (title or "").lower()
    return ("stängd" in t) or ("closed" in t)


def _base(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _with_query(url: str, extra_params: dict) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q.update({k: v for k, v in extra_params.items() if v is not None})
    new_query = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


def _extract_job_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select('a[href*="/jobs/"]'):
        href = a.get("href") or ""
        if not href:
            continue
        if "/applications/" in href:
            continue

        full = href if href.startswith("http") else urljoin(base_url, href)
        if not JOB_PATH_RX.search(urlparse(full).path or ""):
            continue
        links.append(full)

    seen = set()
    out = []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _jsonld_objects(soup: BeautifulSoup) -> list[dict]:
    out = []
    for s in soup.select('script[type="application/ld+json"]'):
        txt = s.get_text(strip=True)
        if not txt:
            continue
        try:
            obj = json.loads(txt)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
            # @graph pattern
            g = obj.get("@graph")
            if isinstance(g, list):
                out.extend([x for x in g if isinstance(x, dict)])
        elif isinstance(obj, list):
            out.extend([x for x in obj if isinstance(x, dict)])
    return out


def _extract_jobposting_jsonld(soup: BeautifulSoup) -> tuple[str, str, str]:
    """
    Returns (title, location, published) from JSON-LD JobPosting when possible.
    """
    title = ""
    location = ""
    published = ""

    for obj in _jsonld_objects(soup):
        typ = str(obj.get("@type", "")).lower()
        if "jobposting" not in typ and typ not in ("job posting", ""):
            continue

        t = obj.get("title")
        if isinstance(t, str) and t.strip():
            title = clean_text(t)

        dp = obj.get("datePosted")
        if isinstance(dp, str) and dp.strip():
            published = dp.strip()

        jl = obj.get("jobLocation")
        if jl:
            locs = jl if isinstance(jl, list) else [jl]
            for loc in locs:
                if not isinstance(loc, dict):
                    continue
                addr = loc.get("address")
                if isinstance(addr, dict):
                    city = clean_text(addr.get("addressLocality") or "")
                    region = clean_text(addr.get("addressRegion") or "")
                    country = clean_text(addr.get("addressCountry") or "")
                    location = clean_text(city or region or country) or location

        if title:
            break

    return title, location, published


def _extract_og_title(soup: BeautifulSoup) -> str:
    m = soup.select_one('meta[property="og:title"]')
    if not m:
        return ""
    c = m.get("content") or ""
    c = clean_text(c)
    # Often "Job title - Q Bemanning"
    if " - " in c:
        c = c.split(" - ")[0].strip()
    return clean_text(c)


def _fetch_detail(sess: requests.Session, job_url: str) -> dict | None:
    try:
        r = sess.get(job_url, timeout=QB_DETAIL_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logger.warning("[qbemanning] detail fetch failed: %s | %s", job_url, e)
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    title, loc, published = _extract_jobposting_jsonld(soup)

    if not title:
        title = _extract_og_title(soup)

    # LAST resort: h1
    if not title:
        h1 = soup.select_one("h1")
        title = clean_text(h1.get_text(" ", strip=True)) if h1 else ""

    title = clean_text(title or "")
    if not title:
        return None

    # Hard stop: cookie-modal titles
    if COOKIE_TITLE_RX.search(title):
        return None

    if _is_closed(title):
        return None

    # Location: if missing or suspiciously long -> Sweden
    loc = clean_text(loc or "")
    if not loc or len(loc) > 60 or COOKIE_TITLE_RX.search(loc):
        loc = "Sweden"
    else:
        # Sweden-safe: keep city but append Sweden if no country marker
        low = loc.lower()
        if not any(x in low for x in ("sweden", "sverige", ", se", " se", "(se)", "swe")):
            loc = f"{loc}, Sweden"

    return {
        "id": None,
        "title": title,
        "company": "Q Bemanning",
        "location": loc,
        "published": published or "",
        "url": job_url,
    }


def fetch(url: str):
    list_url = url or DEFAULT_LIST_URL
    base_url = _base(list_url)

    parsed = urlparse(list_url)
    list_query = parsed.query or ""

    sess = requests.Session()
    sess.headers.update(HEADERS)

    try:
        r = sess.get(list_url, timeout=QB_LIST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logger.warning("[qbemanning] list fetch failed: %s | %s", list_url, e)
        return []

    job_urls = _extract_job_links(r.text, base_url)

    for page in range(2, QB_MAX_PAGES + 1):
        show_more = urljoin(base_url, "/jobs/show_more")
        show_more = _with_query(show_more, {"page": str(page)})

        if list_query:
            show_more = show_more + "&" + list_query

        try:
            rr = sess.get(show_more, timeout=QB_LIST_TIMEOUT)
            if rr.status_code != 200:
                break
            html = rr.text or ""
        except Exception:
            break

        links = _extract_job_links(html, base_url)
        if not links:
            break

        before = len(job_urls)
        for u in links:
            if u not in job_urls:
                job_urls.append(u)

        if len(job_urls) == before:
            break

        if QB_SLEEP:
            time.sleep(QB_SLEEP)

    results = []
    for u in job_urls:
        item = _fetch_detail(sess, u)
        if item:
            results.append(item)
        if QB_SLEEP:
            time.sleep(QB_SLEEP)

    logger.info("[qbemanning] scraped %s items (open only)", len(results))
    return results