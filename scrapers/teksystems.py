import re
from urllib.parse import urljoin

from scrapers._browser import (
    BrowserConfig,
    browser_context,
    goto,
    dismiss_cookie_banners,
    dedup_by_id,
    normalize_title,
)

BASE = "https://careers.teksystems.com"

ENTRYPOINTS = [
    "https://careers.teksystems.com/gb/en/c/developer-jobs?s=1",
    "https://careers.teksystems.com/gb/en/c/project-manager-jobs?s=1",
]

JP_ANCHOR_SEL = 'a[href*="/gb/en/job/JP-"]'
JP_URL_RX = re.compile(r"/gb/en/job/(JP-\d{6,})", re.IGNORECASE)


def _looks_like_listing_url(u: str) -> bool:
    if not u:
        return False
    low = u.lower()
    if "/search-results" in low:
        return False
    return ("/gb/en/c/" in low) or ("/gb/en/" in low)


def _infer_location_from_text(txt: str) -> str:
    """
    Conservative: return ONLY known buckets. Never return the raw text.
    """
    low = (txt or "").lower()
    if "stockholm" in low:
        return "Stockholm"
    if "göteborg" in low or "goteborg" in low or "gothenburg" in low:
        return "Göteborg"
    if "malmö" in low or "malmo" in low:
        return "Malmö"
    if "uppsala" in low:
        return "Uppsala"
    # Optional: remote/distans buckets (if you want them)
    if "remote" in low or "distans" in low or "hybrid" in low:
        # Keep it simple; or return "" if you don't want this category
        return "Remote"
    return ""


def fetch(url: str):
    results = []

    starts = []
    if _looks_like_listing_url(url):
        starts.append(url)
    starts.extend(ENTRYPOINTS)

    with browser_context(
        BrowserConfig(
            headless=True,
            default_timeout_ms=7000,
            navigation_timeout_ms=20000,
            locale="en-GB",
            block_heavy_resources=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
    ) as ctx:
        page = ctx.new_page()

        items = []
        for start in starts[:2]:  # cap attempts
            if not goto(page, start, wait_until="domcontentloaded"):
                continue

            dismiss_cookie_banners(page)

            try:
                page.wait_for_selector(JP_ANCHOR_SEL, timeout=6000)
            except Exception:
                continue

            items = page.eval_on_selector_all(
                JP_ANCHOR_SEL,
                """
                els => {
                  const out = [];
                  const seen = new Set();
                  for (const a of els) {
                    const href = a.getAttribute('href') || '';
                    if (!href.includes('/gb/en/job/JP-')) continue;
                    if (seen.has(href)) continue;
                    seen.add(href);

                    // Title: anchor text / aria-label / title
                    let title = (a.innerText || '').trim();
                    if (!title) title = (a.getAttribute('aria-label') || '').trim();
                    if (!title) title = (a.getAttribute('title') || '').trim();

                    // Nearby card text for location inference (limited)
                    const card = a.closest('article, li, div, section');
                    const blob = card ? (card.innerText || '').slice(0, 800) : '';

                    out.push({ href, title, blob });
                  }
                  return out;
                }
                """,
            ) or []

            if items:
                break

    for it in items[:80]:
        href = (it.get("href") or "").strip()
        title = normalize_title(it.get("title") or "")
        blob = it.get("blob") or ""

        if not href or not title:
            continue

        full_url = href if href.startswith("http") else urljoin(BASE, href)

        m = JP_URL_RX.search(full_url)
        if not m:
            continue
        jp = m.group(1).upper()

        # ✅ IMPORTANT: location is inferred (bucket) or empty – never blob fallback
        location = _infer_location_from_text(blob)

        results.append(
            {
                "id": f"teksystems-{jp}",
                "title": title,
                "company": "TEKsystems",
                "location": location,
                "published": "",
                "url": full_url,
            }
        )

    return dedup_by_id(results)