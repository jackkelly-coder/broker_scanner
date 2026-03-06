import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from utils import generate_id, clean_text, canonicalize_url

ISO_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})T")


def fetch(url):
    """
    Hämtar senaste 3 sidorna från:
    https://upgraded.se/ort/stockholm/
    """
    results = []
    seen = set()

    for page in range(1, 4):
        if page == 1:
            page_url = url
        else:
            page_url = f"{url.rstrip('/')}/page/{page}/"

        try:
            r = requests.get(page_url, timeout=20)
            r.raise_for_status()
        except Exception as e:
            print(f"Error fetching {page_url}: {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.select("h2 a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if "/konsultuppdrag/" not in href:
                continue

            full_url = urljoin(url, href)
            canon = canonicalize_url(full_url)

            if canon in seen:
                continue
            seen.add(canon)

            title = clean_text(a.get_text(strip=True))
            if not title:
                continue

            published = ""
            parent = a.find_parent()
            if parent:
                block_text = clean_text(parent.get_text(" ", strip=True))
                m = ISO_DATE_RE.search(block_text)
                if m:
                    published = m.group(1)

            stable_id = generate_id("upgraded|" + canon)

            results.append({
                "id": stable_id,
                "title": title,
                "company": "Upgraded",
                "location": "Stockholm",
                "published": published,
                "url": canon
            })

    return results