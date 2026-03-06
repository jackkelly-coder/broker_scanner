# scrapers/verama.py

import json
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, unquote

import requests

from utils import clean_text

logger = logging.getLogger(__name__)

DEFAULT_UI_URL = "https://app.verama.com/en/job-requests?page=0&size=20"
DEFAULT_API_URL = "https://app.verama.com/api/public/job-requests?page=0&size=20&sort=firstDayOfApplications,DESC"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; broker-scanner/1.0)"}

VERAMA_SLEEP = float(os.getenv("VERAMA_SLEEP", "0.02"))
VERAMA_TIMEOUT = int(os.getenv("VERAMA_TIMEOUT", "15"))

# Key change: lower default page count so it won't randomly run 5+ minutes
VERAMA_MAX_PAGES = int(os.getenv("VERAMA_MAX_PAGES", "20"))
VERAMA_MAX_ITEMS = int(os.getenv("VERAMA_MAX_ITEMS", "400"))


def _get_list_from_json(data: Any) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []

    for key in ("content", "items", "results", "data"):
        v = data.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    if "page" in data and isinstance(data.get("page"), dict):
        out = _get_list_from_json(data["page"])
        if out:
            return out

    return []


def _extract_title(item: dict) -> str:
    for k in ("title", "name", "headline"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return clean_text(v)
    return ""


def _extract_location(item: dict) -> str:
    candidates = []

    loc = item.get("location")
    if isinstance(loc, dict):
        candidates += [loc.get("city"), loc.get("addressLocality"), loc.get("name"), loc.get("country"), loc.get("countryCode")]
    elif isinstance(loc, str):
        candidates.append(loc)

    addr = item.get("address")
    if isinstance(addr, dict):
        candidates += [addr.get("city"), addr.get("addressLocality"), addr.get("country"), addr.get("countryCode")]

    candidates += [item.get("city"), item.get("region"), item.get("country"), item.get("countryCode")]

    for c in candidates:
        if not c:
            continue
        s = clean_text(str(c))
        if s:
            return s
    return ""


def _extract_url(item: dict) -> str:
    for k in ("url", "href", "permalink"):
        v = item.get(k)
        if isinstance(v, str) and v.startswith("http"):
            return v

    raw_id = item.get("id") or item.get("jobRequestId") or item.get("jobId")
    if raw_id is None:
        return ""
    return f"https://app.verama.com/en/job-requests/{raw_id}"


def _extract_published(item: dict) -> str:
    for k in ("published", "datePosted", "createdAt", "postedAt", "firstDayOfApplications"):
        v = item.get(k)
        if v:
            return clean_text(str(v))
    return ""


def _is_api_url(u: str) -> bool:
    return "/api/public/job-requests" in (u or "")


def _set_page(u: str, page: int) -> str:
    p = urlparse(u)
    qs = parse_qs(p.query, keep_blank_values=True)
    qs["page"] = [str(page)]
    if "size" not in qs:
        qs["size"] = ["20"]
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(qs, doseq=True), p.fragment))


def _to_str_or_empty(v) -> str:
    return "" if v is None else str(v)


def _ui_to_api(ui_url: str) -> str:
    ui_url = (ui_url or "").strip()
    if not ui_url:
        return DEFAULT_API_URL

    p = urlparse(ui_url)
    qs = parse_qs(p.query, keep_blank_values=True)

    api_qs = {}

    api_qs["page"] = (qs.get("page", ["0"])[0] or "0")
    api_qs["size"] = (qs.get("size", ["20"])[0] or "20")

    raw_fc = (qs.get("filtersConfig", [""])[0] or "").strip()
    if raw_fc:
        try:
            fc = json.loads(unquote(raw_fc))
        except Exception:
            fc = None

        if isinstance(fc, dict):
            loc = fc.get("location")
            if isinstance(loc, dict):
                for k in ("id", "signature", "city", "country", "name", "locationId", "countryCode", "suggestedPhoneCode", "radius"):
                    if k in loc:
                        api_qs[f"location.{k}"] = _to_str_or_empty(loc.get(k))

            if "query" in fc:
                api_qs["query"] = _to_str_or_empty(fc.get("query"))

            for k in ("dedicated", "favouritesOnly", "recommendedOnly"):
                if k in fc:
                    v = fc.get(k)
                    if isinstance(v, bool):
                        api_qs[k] = "true" if v else "false"
                    else:
                        api_qs[k] = _to_str_or_empty(v)

    api_qs["sort"] = "firstDayOfApplications,DESC"

    return urlunparse((p.scheme, p.netloc, "/api/public/job-requests", "", urlencode(api_qs, doseq=False), ""))


def _normalize_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return DEFAULT_API_URL
    if _is_api_url(u):
        return u
    if "/job-requests" in u:
        return _ui_to_api(u)
    return DEFAULT_API_URL


def fetch(url: str):
    api_url = _normalize_url(url)
    logger.info("[verama] using api_url=%s", api_url)

    sess = requests.Session()
    sess.headers.update(HEADERS)

    all_rows = []
    seen_ids = set()

    for page in range(0, VERAMA_MAX_PAGES):
        if VERAMA_MAX_ITEMS and len(all_rows) >= VERAMA_MAX_ITEMS:
            logger.info("[verama] stop: reached VERAMA_MAX_ITEMS=%s", VERAMA_MAX_ITEMS)
            break

        page_url = _set_page(api_url, page)

        try:
            r = sess.get(page_url, timeout=VERAMA_TIMEOUT)
        except Exception as e:
            logger.warning("[verama] API request failed page=%s: %s", page, e)
            break

        if r.status_code != 200:
            logger.warning("[verama] API bad status page=%s status=%s url=%s", page, r.status_code, page_url)
            break

        ctype = (r.headers.get("content-type") or "").lower()
        text = r.text or ""

        if "json" not in ctype:
            snippet = text[:200].replace("\n", " ").replace("\r", " ")
            logger.warning("[verama] non-json response page=%s ctype=%s snippet=%r url=%s", page, ctype, snippet, page_url)
            break

        try:
            data = r.json()
        except Exception as e:
            snippet = text[:200].replace("\n", " ").replace("\r", " ")
            logger.warning("[verama] json parse failed page=%s err=%s snippet=%r url=%s", page, e, snippet, page_url)
            break

        items = _get_list_from_json(data)
        if not items:
            break

        for it in items:
            raw_id = it.get("id") or it.get("jobRequestId") or it.get("jobId")
            title = _extract_title(it)
            if raw_id is None or not title:
                continue

            rid = f"verama-{raw_id}"
            if rid in seen_ids:
                continue
            seen_ids.add(rid)

            loc = _extract_location(it) or "Sweden"

            all_rows.append(
                {
                    "id": rid,
                    "title": title,
                    "company": "Verama",
                    "location": loc,
                    "published": _extract_published(it),
                    "url": _extract_url(it),
                }
            )

        size = 0
        try:
            size = int(parse_qs(urlparse(page_url).query).get("size", ["0"])[0])
        except Exception:
            size = 0
        if size and len(items) < size:
            break

        if VERAMA_SLEEP:
            time.sleep(VERAMA_SLEEP)

    logger.info("[verama] scraped %s items via API", len(all_rows))
    return all_rows