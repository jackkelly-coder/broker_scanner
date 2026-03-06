import re
import time
from urllib.parse import urljoin

from utils import generate_id, clean_text, canonicalize_url
from utils_debug import dump_html

from scrapers._browser import (
    BrowserConfig,
    browser_context,
    goto,
    safe_text,
    safe_attr,
    dedup_by_id,
)

DEBUG = False  # True = open detail pages to infer location (slower)

DEFAULT_LIST_URL = "https://www.headagent.se/"  # if your config url is empty/odd, we start here


def _strip_trailing_location_from_title(title: str) -> str:
    if not title:
        return title

    t = clean_text(title)

    locations = [
        "Stockholm",
        "Solna",
        "Sundbyberg",
        "Södertälje",
        "Sodertalje",
        "Uppsala",
        "Göteborg",
        "Goteborg",
        "Malmö",
        "Malmo",
        "Sverige",
        "Sweden",
    ]

    for loc in locations:
        t2 = re.sub(rf"\s+{re.escape(loc)}\s*$", "", t, flags=re.IGNORECASE).strip()
        if t2 != t:
            t = t2

    for loc in locations:
        t2 = re.sub(
            rf"([A-Za-zÅÄÖåäö]{{2,}}){re.escape(loc)}\s*$",
            r"\1",
            t,
            flags=re.IGNORECASE,
        ).strip()
        if t2 != t:
            t = t2

    return t


def _infer_location_from_text(txt: str) -> str:
    low = (txt or "").lower()
    if any(x in low for x in ["stockholm", "stockholms", "sthlm"]):
        return "Stockholm"
    if "uppsala" in low:
        return "Uppsala"
    if any(x in low for x in ["södertälje", "sodertalje"]):
        return "Södertälje"
    if any(x in low for x in ["göteborg", "goteborg"]):
        return "Göteborg"
    if any(x in low for x in ["malmö", "malmo"]):
        return "Malmö"
    if any(x in low for x in ["sverige", "sweden"]):
        return "Sverige"
    return ""


def _best_frame_for_jobs(page):
    """
    Choose the frame that contains the most job-like links.
    """
    best = None
    best_score = -1
    for fr in page.frames:
        try:
            c1 = fr.locator('a[href*="/lediga-jobb/"]').count()
            c2 = fr.locator("a[href]").count()
            score = (c1 * 10) + min(c2, 50)
            if score > best_score:
                best_score = score
                best = fr
        except Exception:
            continue
    return best, best_score


def _wait_for_job_frame(page, timeout_s: float = 6.0, poll_s: float = 0.35):
    """
    Poll frames until we see at least one frame with /lediga-jobb/ links.
    This is the key fix vs. 'found=0' due to iframe loading after DOMContentLoaded.
    """
    deadline = time.time() + timeout_s
    best = None
    best_score = -1

    while time.time() < deadline:
        fr, score = _best_frame_for_jobs(page)
        if fr is not None and score > best_score:
            best, best_score = fr, score

        # If we already have job links, we can stop
        try:
            if best is not None and best.locator('a[href*="/lediga-jobb/"]').count() > 0:
                return best
        except Exception:
            pass

        page.wait_for_timeout(int(poll_s * 1000))

    return best


def _collect_candidates_from_frame(frame, base_url: str) -> list[tuple[str, str]]:
    candidates = []
    seen = set()

    anchors = frame.locator('a[href*="/lediga-jobb/"]')
    n = min(anchors.count(), 400)

    for i in range(n):
        a = anchors.nth(i)

        href = safe_attr(a, "href", timeout_ms=700)
        if not href:
            continue

        full_url = urljoin(base_url, href)
        low_url = full_url.lower()

        # Drop obvious non-jobs
        if any(x in low_url for x in ["#pll_switcher", "/uppdrag/", "/tillsatta-uppdrag/"]):
            continue

        if full_url in seen:
            continue
        seen.add(full_url)

        text = clean_text(safe_text(a, timeout_ms=800))
        if not text:
            text = clean_text(
                safe_attr(a, "aria-label", timeout_ms=500) or safe_attr(a, "title", timeout_ms=500)
            )

        if not text:
            continue

        low_text = text.lower()
        if low_text in {"fortsätt", "läs mer"}:
            continue
        if any(x in low_text for x in ["consent", "svenska", "english", "referensuppdrag"]):
            continue

        candidates.append((full_url, text))

    return candidates


def _collect_candidates_from_all_frames(page, base_url: str) -> list[tuple[str, str]]:
    """
    Fallback: scan all frames (still cheap) if best frame yields 0.
    """
    out = []
    seen = set()
    for fr in page.frames:
        try:
            cands = _collect_candidates_from_frame(fr, base_url)
            for u, t in cands:
                if u in seen:
                    continue
                seen.add(u)
                out.append((u, t))
        except Exception:
            continue
    return out


def fetch(url: str):
    results = []
    start_url = url or DEFAULT_LIST_URL

    with browser_context(
        BrowserConfig(
            headless=True,
            default_timeout_ms=8000,
            navigation_timeout_ms=20000,
            locale="sv-SE",
            block_heavy_resources=True,
        )
    ) as ctx:
        page = ctx.new_page()
        page.set_extra_http_headers({"Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"})

        if not goto(page, start_url, wait_until="domcontentloaded"):
            return []

        # Important: iframe content often loads *after* DOMContentLoaded
        target = _wait_for_job_frame(page, timeout_s=6.0, poll_s=0.35)
        if target is None:
            return []

        # Optional debug
        try:
            dump_html("headagent_best_frame", target.content())
        except Exception:
            pass

        candidates = _collect_candidates_from_frame(target, start_url)
        if not candidates:
            candidates = _collect_candidates_from_all_frames(page, start_url)

        if not candidates:
            return []

        for full_url, title in candidates:
            location = _infer_location_from_text(title)
            clean_title = _strip_trailing_location_from_title(title)

            canon = canonicalize_url(full_url)
            stable_id = generate_id("headagent|" + canon)

            results.append(
                {
                    "id": stable_id,
                    "title": clean_text(clean_title),
                    "company": "HeadAgent",
                    "location": clean_text(location),
                    "published": "",
                    "url": canon,
                }
            )

    return dedup_by_id(results)