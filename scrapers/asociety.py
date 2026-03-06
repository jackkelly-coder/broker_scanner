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


def parse_detail(page, url: str) -> tuple[str, str]:
    """
    Läser detaljsidan och plockar en ren titel (H1) + enkel location.
    """
    if not goto(page, url, wait_until="domcontentloaded"):
        return "", ""
    page.wait_for_timeout(400)

    title = clean_text(safe_text(page.locator("h1").first, timeout_ms=1500))
    if not title:
        try:
            title = clean_text(page.title() or "")
        except Exception:
            title = ""

    location = ""
    body = (safe_text(page.locator("body"), timeout_ms=2000) or "").lower()

    cities = [
        ("stockholm", "Stockholm"),
        ("sthlm", "Stockholm"),
        ("göteborg", "Göteborg"),
        ("goteborg", "Göteborg"),
        ("malmö", "Malmö"),
        ("malmo", "Malmö"),
        ("uppsala", "Uppsala"),
        ("norrköping", "Norrköping"),
        ("norrkoping", "Norrköping"),
        ("linköping", "Linköping"),
        ("linkoping", "Linköping"),
        ("luleå", "Luleå"),
        ("lulea", "Luleå"),
        ("karlskrona", "Karlskrona"),
        ("arboga", "Arboga"),
        ("karlskoga", "Karlskoga"),
        ("lund", "Lund"),
    ]
    for needle, nice in cities:
        if needle in body:
            location = nice
            break

    return title, location


def fetch(url):
    results = []
    seen = set()

    with browser_context(BrowserConfig(headless=True, default_timeout_ms=9000, navigation_timeout_ms=20000, locale="sv-SE")) as ctx:
        page = ctx.new_page()
        page.set_extra_http_headers({"Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"})

        if not goto(page, url, wait_until="domcontentloaded"):
            return []

        page.wait_for_timeout(1200)

        html = page.content()
        dump_html("asociety", html)

        links = page.locator("a[href]")
        count = min(links.count(), 800)

        candidates = []
        for i in range(count):
            a = links.nth(i)
            href = (safe_attr(a, "href", timeout_ms=700) or "").strip()
            if not href:
                continue

            if "/uppdrag" not in href:
                continue

            full_url = urljoin(url, href)

            # Skippa list-/språksidor
            if full_url.rstrip("/") in {
                url.rstrip("/"),
                "https://www.asocietygroup.com/en/uppdrag",
                "https://www.asocietygroup.com/en/uppdrag/",
                "https://www.asocietygroup.com/sv/uppdrag",
                "https://www.asocietygroup.com/sv/uppdrag/",
            }:
                continue

            if full_url in seen:
                continue

            text = clean_text(safe_text(a, timeout_ms=700) or "")
            if text.lower() in {"english", "svenska"}:
                continue

            seen.add(full_url)
            candidates.append(full_url)

        detail = ctx.new_page()
        detail.set_extra_http_headers({"Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"})

        for full_url in candidates:
            try:
                title, location = parse_detail(detail, full_url)
            except Exception:
                continue

            if not title:
                continue

            canon = canonicalize_url(full_url)
            stable_id = generate_id("asociety|" + canon)

            results.append({
                "id": stable_id,
                "title": title,
                "company": "A Society",
                "location": location,
                "published": "",
                "url": canon
            })

        detail.close()

    return dedup_by_id(results)