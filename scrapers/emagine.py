# scrapers/emagine.py
import json
import logging
import os
import re
import time

import requests

from utils import clean_text, canonicalize_url
from geo import is_sweden_assignment
from scrapers._browser import (
    BrowserConfig,
    browser_context,
    goto,
    dedup_by_id,
)

logger = logging.getLogger(__name__)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))

API_SEARCH = "https://portal-api.emagine.org/api/JobAds/Search"
PUBLIC_BASE = "https://portal.emagine.org"

EMAGINE_PAGE_SIZE = int(os.getenv("EMAGINE_PAGE_SIZE", "100"))
EMAGINE_SLEEP = float(os.getenv("EMAGINE_SLEEP", "0.02"))

# New: try API-first (avoid Playwright hangs)
EMAGINE_API_FIRST = os.getenv("EMAGINE_API_FIRST", "1").strip().lower() in ("1", "true", "yes", "y", "on")
EMAGINE_API_TIMEOUT = float(os.getenv("EMAGINE_API_TIMEOUT", "20"))

# New: cap Playwright time so it never eats your whole run
EMAGINE_BROWSER_MAX_SECONDS = float(os.getenv("EMAGINE_BROWSER_MAX_SECONDS", "20"))

EMAGINE_TRY_PAYLOAD_SWEDEN = os.getenv("EMAGINE_TRY_PAYLOAD_SWEDEN", "1").strip().lower() in ("1", "true", "yes", "y", "on")
EMAGINE_LOCAL_SWEDEN_FILTER = os.getenv("EMAGINE_LOCAL_SWEDEN_FILTER", "1").strip().lower() in ("1", "true", "yes", "y", "on")


def _best(d: dict, keys: list[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = clean_text(str(v))
        if s:
            return s
    return ""


def _extract_list(obj):
    if obj is None:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for k in ("items", "results", "jobs", "jobAds", "records"):
            v = obj.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        data = obj.get("data")
        if isinstance(data, dict):
            for k in ("items", "results", "jobAds", "records"):
                v = data.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        for v in obj.values():
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _extract_total(obj) -> int | None:
    if not isinstance(obj, dict):
        return None
    for k in ("totalCount", "total", "totalHits", "count"):
        v = obj.get(k)
        if isinstance(v, int):
            return v
    data = obj.get("data")
    if isinstance(data, dict):
        for k in ("totalCount", "total", "totalHits", "count"):
            v = data.get(k)
            if isinstance(v, int):
                return v
    return None


def _extract_location(job: dict) -> str:
    wl = job.get("jobAdWorkLocation")
    if isinstance(wl, dict):
        city = _best(wl, ["city", "cityName", "name"])
        region = _best(wl, ["region", "regionName"])
        country = _best(wl, ["country", "countryName"])
        return clean_text(city or region or country)
    if isinstance(wl, str):
        return clean_text(wl)

    loc = _best(job, [
        "location", "locationName", "location_name",
        "city", "cityName",
        "region", "regionName",
        "country", "countryName",
    ])
    return clean_text(loc)


def _headers_from_playwright(req_headers: dict) -> dict:
    if not req_headers:
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; broker-scanner/1.0)",
        }

    out = {}
    for k, v in req_headers.items():
        lk = k.lower().strip()
        if lk in {
            "host", "content-length", "connection", "accept-encoding",
            "sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest",
            "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
        }:
            continue
        if lk in {"accept", "content-type", "accept-language", "origin", "referer", "user-agent"}:
            out[k] = v
            continue
        if lk == "authorization":
            out[k] = v
            continue
        if "csrf" in lk or "xsrf" in lk or "verification" in lk:
            out[k] = v
            continue
        if lk.startswith("x-"):
            out[k] = v
            continue

    out.setdefault("Content-Type", "application/json")
    out.setdefault("Accept", "application/json, text/plain, */*")
    out.setdefault("User-Agent", "Mozilla/5.0 (compatible; broker-scanner/1.0)")
    return out


def _try_apply_sweden_filter_to_payload(payload: dict) -> tuple[dict, bool]:
    if not isinstance(payload, dict):
        return payload, False

    p = dict(payload)

    f = p.get("filter")
    if isinstance(f, dict):
        f2 = dict(f)
        for key in ("country", "countryName", "country_name"):
            if key in f2 and str(f2.get(key) or "").strip():
                return p, False
        f2["country"] = "Sweden"
        p["filter"] = f2
        return p, True

    wl = p.get("jobAdWorkLocation")
    if isinstance(wl, dict):
        wl2 = dict(wl)
        if not (wl2.get("country") or wl2.get("countryName")):
            wl2["country"] = "Sweden"
            p["jobAdWorkLocation"] = wl2
            return p, True

    fl = p.get("filters")
    if isinstance(fl, list):
        fl2 = list(fl)
        already = False
        for it in fl2:
            if not isinstance(it, dict):
                continue
            k = str(it.get("field") or it.get("name") or "").lower()
            v = str(it.get("value") or it.get("values") or "").lower()
            if "country" in k and "sweden" in v:
                already = True
                break
        if not already:
            fl2.append({"field": "country", "value": "Sweden"})
            p["filters"] = fl2
            return p, True

    return payload, False


def _try_paginate_requests(payload: dict, headers: dict | None = None, cookies: dict | None = None) -> list[dict]:
    sess = requests.Session()
    sess.headers.update(_headers_from_playwright(headers or {}))
    if cookies:
        sess.cookies.update(cookies)

    def call(p: dict) -> tuple[list[dict], int | None]:
        r = sess.post(API_SEARCH, json=p, timeout=EMAGINE_API_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return _extract_list(data), _extract_total(data)

    base_payload = dict(payload)

    if EMAGINE_TRY_PAYLOAD_SWEDEN:
        patched, applied = _try_apply_sweden_filter_to_payload(base_payload)
        if applied:
            base_payload = patched
            logger.info("[emagine] Applied Sweden filter to API payload (heuristic).")
        else:
            logger.info("[emagine] Could not apply Sweden filter to payload; will filter locally.")

    all_items: list[dict] = []
    seen_ids: set[str] = set()
    total_count: int | None = None

    def get_id(it: dict) -> str:
        return (
            _best(it, ["id", "jobId", "job_id"])
            or _best(it, ["requestId"])
            or json.dumps(it, sort_keys=True)[:120]
        )

    for page_index in range(0, 1000):
        p2 = dict(base_payload)
        p2["skipCount"] = page_index * EMAGINE_PAGE_SIZE
        p2["maxResultCount"] = EMAGINE_PAGE_SIZE

        items, total = call(p2)
        if total_count is None and isinstance(total, int):
            total_count = total

        if not items:
            break

        if EMAGINE_LOCAL_SWEDEN_FILTER:
            filtered = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                loc = _extract_location(it)
                title = _best(it, ["title", "name", "position", "role", "headline"])
                if is_sweden_assignment(loc, title):
                    filtered.append(it)
            items = filtered

        for it in items:
            if not isinstance(it, dict):
                continue
            jid = get_id(it)
            if jid in seen_ids:
                continue
            seen_ids.add(jid)
            all_items.append(it)

        if total_count is not None and (page_index + 1) * EMAGINE_PAGE_SIZE >= total_count:
            break
        if total_count is None and len(items) < EMAGINE_PAGE_SIZE:
            break

        if EMAGINE_SLEEP > 0:
            time.sleep(EMAGINE_SLEEP)

    return all_items


def _stable_job_url(job: dict) -> str:
    jid = _best(job, ["id", "jobId", "job_id"])
    rid = _best(job, ["requestId", "request_id"])
    if jid:
        return canonicalize_url(f"{PUBLIC_BASE}/job/{jid}")
    if rid:
        return canonicalize_url(f"{PUBLIC_BASE}/request/{rid}")
    blob = json.dumps(job, sort_keys=True)
    key = re.sub(r"[^a-zA-Z0-9]+", "", blob)[:32]
    return canonicalize_url(f"{PUBLIC_BASE}/job/unknown-{key}")


def _job_id(job: dict, job_url: str) -> str:
    jid = _best(job, ["id", "jobId", "job_id"])
    if jid:
        return f"emagine-{jid}"
    rid = _best(job, ["requestId", "request_id"])
    if rid:
        return f"emagine-{rid}"
    return "emagine-" + re.sub(r"[^a-zA-Z0-9]+", "", job_url)[-24:]


def _direct_api_payload() -> dict:
    """
    Minimal best-effort payload that often works without browser capture.
    If API rejects it, we fall back to Playwright capture.
    """
    return {
        "filter": {"country": "Sweden"},
        "skipCount": 0,
        "maxResultCount": EMAGINE_PAGE_SIZE,
    }


def fetch(url: str):
    # 1) Try API-first: avoids Playwright hangs
    if EMAGINE_API_FIRST:
        try:
            items = _try_paginate_requests(_direct_api_payload(), headers=None, cookies=None)
            if items:
                logger.info("[emagine] API-first succeeded: %s items", len(items))
                return _to_results(items)
            logger.info("[emagine] API-first returned 0 items, falling back to browser capture.")
        except Exception as e:
            logger.warning("[emagine] API-first failed (%s). Falling back to browser capture.", e)

    # 2) Browser capture fallback (hard-capped)
    captured_payload: dict | None = None
    captured_headers: dict | None = None
    captured_cookies: dict = {}

    start = time.monotonic()
    deadline = start + EMAGINE_BROWSER_MAX_SECONDS

    with browser_context(
        BrowserConfig(
            headless=True,
            default_timeout_ms=9000,
            navigation_timeout_ms=25000,
            locale="en-GB",
        )
    ) as ctx:
        page = ctx.new_page()
        page.set_extra_http_headers({"Accept-Language": "en-GB,en;q=0.9,sv;q=0.8"})

        cdp = None
        try:
            cdp = page.context.new_cdp_session(page)
            cdp.send("Network.enable", {})
        except Exception as e:
            cdp = None
            logger.warning("[emagine] CDP not available: %s", e)

        def on_cdp_request_will_be_sent(params: dict):
            nonlocal captured_payload, captured_headers
            try:
                if isinstance(captured_payload, dict):
                    return

                req = params.get("request") or {}
                url_ = req.get("url") or ""
                method = (req.get("method") or "").upper()
                if method != "POST":
                    return
                if not url_.startswith(API_SEARCH):
                    return

                if captured_headers is None:
                    h = req.get("headers")
                    if isinstance(h, dict):
                        captured_headers = {str(k): str(v) for k, v in h.items()}

                request_id = params.get("requestId")
                if not request_id or cdp is None:
                    return

                out = cdp.send("Network.getRequestPostData", {"requestId": request_id})
                post_data = out.get("postData") if isinstance(out, dict) else None

                if post_data and isinstance(post_data, str):
                    try:
                        captured_payload = json.loads(post_data.strip())
                    except Exception:
                        captured_payload = None
            except Exception:
                return

        if cdp is not None:
            try:
                cdp.on("Network.requestWillBeSent", on_cdp_request_will_be_sent)
            except Exception:
                pass

        # If goto hangs in wrapper, our main hard-timeout still protects us,
        # but we also keep the rest of the browser logic short.
        if not goto(page, url, wait_until="domcontentloaded"):
            return []

        # Wait for payload/headers, but never longer than EMAGINE_BROWSER_MAX_SECONDS total
        while time.monotonic() < deadline:
            if isinstance(captured_payload, dict) and captured_headers:
                break
            page.wait_for_timeout(250)

        try:
            for c in ctx.cookies():
                n = c.get("name")
                v = c.get("value")
                if n and v:
                    captured_cookies[n] = v
        except Exception:
            captured_cookies = {}

    if not isinstance(captured_payload, dict) or not captured_headers:
        logger.warning("[emagine] missing payload/headers after browser capture -> abort")
        return []

    items = _try_paginate_requests(captured_payload, captured_headers, captured_cookies)
    logger.info("[emagine] fetched %s items (page_size=%s sleep=%s)", len(items), EMAGINE_PAGE_SIZE, EMAGINE_SLEEP)
    return _to_results(items)


def _to_results(items: list[dict]) -> list[dict]:
    results = []
    for job in items:
        if not isinstance(job, dict):
            continue

        title = _best(job, ["title", "name", "position", "role", "headline"])
        if not title:
            continue

        loc = _extract_location(job)
        published = _best(job, ["applicationDate", "published", "publishedAt", "createdAt", "date"])

        job_url = _stable_job_url(job)
        jid = _job_id(job, job_url)

        results.append({
            "id": jid,
            "title": title,
            "company": "emagine",
            "location": clean_text(loc),
            "published": published,
            "url": job_url,
            "emagine_job_id": str(job.get("id", "")),
            "emagine_request_id": str(job.get("requestId", "")),
        })

    return dedup_by_id(results)