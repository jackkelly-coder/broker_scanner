import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from utils import generate_id, clean_text, canonicalize_url

STOCKHOLM_RE = re.compile(r"\bstockholm\b", re.IGNORECASE)


def _extract_location(detail_soup: BeautifulSoup) -> str:
    """
    Försöker hitta ort i detaljsidan via vanliga mönster.
    Faller tillbaka till att leta i sidans text.
    """
    candidates = []

    # 1) Vanliga "location"-klasser
    for sel in [
        "[class*='location']",
        "[class*='place']",
        "[class*='city']",
        "[class*='ort']",
        "[class*='plats']",
    ]:
        el = detail_soup.select_one(sel)
        if el:
            candidates.append(clean_text(el.get_text(" ", strip=True)))

    # 2) Leta efter labels i text (Ort/Plats)
    text = clean_text(detail_soup.get_text(" ", strip=True))
    m = re.search(r"\b(Ort|Plats)\s*:\s*([A-Za-zÅÄÖåäö \-]+)", text)
    if m:
        candidates.append(clean_text(m.group(2)))

    # 3) Sista fallback: om Stockholm nämns någonstans, sätt Stockholm
    if STOCKHOLM_RE.search(text):
        candidates.append("Stockholm")

    # Returnera första rimliga kandidat
    for c in candidates:
        if c and len(c) <= 60:
            return c
    return ""


def fetch(url):
    results = []

    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return results

    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/aktuellauppdrag/" not in href:
            continue

        full_url = urljoin(url, href)
        if full_url.rstrip("/") == url.rstrip("/"):
            continue
        if full_url in seen:
            continue

        title = clean_text(a.get_text(strip=True))
        if not title:
            continue

        seen.add(full_url)

        # Hämta detaljsidan för att hitta location (best-effort)
        location = ""
        try:
            d = requests.get(full_url, timeout=15)
            d.raise_for_status()
            detail_soup = BeautifulSoup(d.text, "html.parser")
            location = _extract_location(detail_soup)
        except Exception:
            location = ""

        canon = canonicalize_url(full_url)
        stable_id = generate_id("bohmans|" + canon)

        results.append({
            "id": stable_id,
            "title": title,
            "company": "Bohmans",
            "location": location,
            "published": "",
            "url": canon
        })

    return results