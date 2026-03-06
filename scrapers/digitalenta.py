import json
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from scrapers._browser import (
    BrowserConfig,
    browser_context,
    goto,
    dismiss_cookie_banners,
    dedup_by_id,
    normalize_title,
    normalize_location,
)

BASE = "https://karriar.digitalenta.se"


def _block_heavy_resources(ctx):
    def handler(route):
        try:
            rtype = route.request.resource_type
            if rtype in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()
        except Exception:
            try:
                route.continue_()
            except Exception:
                pass

    try:
        ctx.route("**/*", handler)
    except Exception:
        pass


def _collect_job_hrefs(page) -> list[str]:
    try:
        hrefs = page.eval_on_selector_all(
            "a[href]",
            """
            els => {
              const hs = els.map(e => e.getAttribute('href')).filter(Boolean);
              const filtered = hs.filter(h => h.includes('/jobs/'));
              return Array.from(new Set(filtered));
            }
            """,
        )
        return hrefs or []
    except Exception:
        return []


def _is_cookie_noise(title: str) -> bool:
    low = (title or "").strip().lower()
    needles = [
        "välj vilka cookies",
        "cookies du vill",
        "godkänna",
        "cookie",
        "cookies",
        "integritetsinställningar",
    ]
    return any(n in low for n in needles)


def _iter_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_dicts(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from _iter_dicts(x)


def _extract_jobposting_from_html(html: str) -> dict:
    """
    Robust: plocka JobPosting ur JSON-LD.
    Returnerar {"title": "", "published": "", "location": ""}.
    """
    out = {"title": "", "published": "", "location": ""}

    if not html:
        return out

    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.select('script[type="application/ld+json"]')
    for s in scripts[:20]:
        raw = (s.get_text() or "").strip()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except Exception:
            continue

        jp = None
        for d in _iter_dicts(data):
            t = d.get("@type") or d.get("type")
            if t == "JobPosting":
                jp = d
                break
        if not jp:
            continue

        out["title"] = (jp.get("title") or "").strip()
        out["published"] = (jp.get("datePosted") or jp.get("datePublished") or "").strip()

        # jobLocation kan vara dict eller list
        def loc_to_str(loc_obj):
            if not isinstance(loc_obj, dict):
                return ""
            addr = loc_obj.get("address")
            if isinstance(addr, dict):
                parts = [
                    addr.get("addressLocality"),
                    addr.get("addressRegion"),
                    addr.get("addressCountry"),
                ]
                parts = [p for p in parts if isinstance(p, str) and p.strip()]
                return ", ".join(parts).strip()
            return ""

        loc = jp.get("jobLocation")
        loc_str = ""
        if isinstance(loc, list):
            for l in loc:
                s2 = loc_to_str(l)
                if s2:
                    loc_str = s2
                    break
        elif isinstance(loc, dict):
            loc_str = loc_to_str(loc)

        out["location"] = loc_str
        return out

    return out


def _make_abs_job_url(href: str) -> str:
    return href if href.startswith("http") else urljoin(BASE, href)


def _is_our_domain(full_url: str) -> bool:
    try:
        host = urlparse(full_url).netloc.lower()
        return "digitalenta.se" in host
    except Exception:
        return True


def fetch(url: str):
    results = []

    entrypoints = [
        urljoin(BASE, "/jobs"),
        urljoin(BASE, "/#jobs"),
        BASE,
    ]

    conf = BrowserConfig(headless=True, default_timeout_ms=8000, navigation_timeout_ms=15000)

    # 1) Playwright: bara för att samla hrefs (CSR / cookie overlays)
    with browser_context(conf) as ctx:
        _block_heavy_resources(ctx)

        page = ctx.new_page()
        hrefs: list[str] = []

        for ep in entrypoints:
            if not goto(page, ep, wait_until="domcontentloaded"):
                continue

            dismiss_cookie_banners(page)

            try:
                page.wait_for_selector('a[href*="/jobs/"]', timeout=6000)
            except Exception:
                pass

            hrefs = _collect_job_hrefs(page)
            if hrefs:
                break

    if not hrefs:
        return []

    # 2) Requests: snabb HTML-fetch + JSON-LD parse (ingen Playwright per detail)
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    for href in hrefs:
        full_url = _make_abs_job_url(href)
        if not _is_our_domain(full_url):
            continue

        m = re.search(r"/jobs/(\d+)", full_url)
        job_num_id = m.group(1) if m else re.sub(r"[^a-zA-Z0-9]+", "-", full_url).strip("-")
        job_id = f"digitalenta-{job_num_id}"

        try:
            resp = sess.get(full_url, timeout=12)
            if resp.status_code != 200:
                continue
            html = resp.text
        except Exception:
            continue

        jp = _extract_jobposting_from_html(html)
        title = normalize_title(jp.get("title") or "")
        published = (jp.get("published") or "").strip()
        location = normalize_location(jp.get("location") or "")

        # fallback: HTML <title> om JSON-LD saknar title
        if not title:
            soup = BeautifulSoup(html, "html.parser")
            t = (soup.title.get_text().strip() if soup.title else "")
            t = (t.split("|")[0] if "|" in t else t).strip()
            t = normalize_title(t)
            if t and not _is_cookie_noise(t):
                title = t

        if not title or _is_cookie_noise(title):
            continue

        results.append(
            {
                "id": job_id,
                "title": title,
                "company": "Digitalenta",
                "location": location,
                "published": published,
                "url": full_url,
            }
        )

    return dedup_by_id(results)