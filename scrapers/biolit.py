import re
import requests
from bs4 import BeautifulSoup

from utils import clean_text, canonicalize_url


INCOMING_DATE_RX = re.compile(r"\bInkom\s*:\s*(\d{4}-\d{2}-\d{2})\b", re.IGNORECASE)
ASSIGNMENT_NO_RX = re.compile(r"\bUppdragsnummer\s*:\s*([0-9]{3,6})\b", re.IGNORECASE)
PLACE_RX = re.compile(r"\bPlats\s*:\s*([^\n\r]+)", re.IGNORECASE)


def _infer_location_from_block(block_text: str) -> str:
    if not block_text:
        return ""

    m = PLACE_RX.search(block_text)
    if m:
        loc = clean_text(m.group(1))
        return loc[:80]

    low = block_text.lower()
    if "stockholm" in low or "sthlm" in low:
        return "Stockholm"
    if "solna" in low:
        return "Solna"
    if "sundbyberg" in low:
        return "Sundbyberg"
    if "södertälje" in low or "sodertalje" in low:
        return "Södertälje"
    if "göteborg" in low or "goteborg" in low:
        return "Göteborg"
    if "malmö" in low or "malmo" in low:
        return "Malmö"
    if "uppsala" in low:
        return "Uppsala"

    return ""


def _is_probable_title(line: str) -> bool:
    """
    Heuristic: titles are usually short-ish, not boilerplate, and not containing 'Inkom'/'Uppdragsnummer'.
    """
    if not line:
        return False
    low = line.lower()

    if "inkom" in low or "uppdragsnummer" in low:
        return False
    if low in {
        "aktuella konsultuppdrag",
        "konsultuppdrag",
        "biolit",
        "kontakt",
        "intresserad kontakta",
    }:
        return False

    # too long -> probably body text
    if len(line) > 90:
        return False

    # avoid lines that look like phone/email
    if "@" in line or "070" in line or "073" in line or "08-" in line:
        return False

    # needs some letters
    return any(ch.isalpha() for ch in line)


def fetch(url: str):
    """
    Biolit konsultuppdrag is a single long page.
    The Inkom/date and Uppdragsnummer may be split across lines, so we:
      - find 'Inkom: YYYY-MM-DD' line
      - look ahead for 'Uppdragsnummer: NNNN' within next few lines
      - use the nearest probable title line above as title
    """
    results = []

    try:
        r = requests.get(
            url,
            timeout=20,
            headers={"Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"},
        )
        r.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return results

    soup = BeautifulSoup(r.text, "html.parser")

    # Ensure <br> becomes newlines
    for br in soup.find_all("br"):
        br.replace_with("\n")

    raw_text = soup.get_text("\n")
    lines = [clean_text(x) for x in raw_text.split("\n")]
    lines = [x for x in lines if x]

    canon_base = canonicalize_url(url)

    i = 0
    while i < len(lines):
        line = lines[i]
        m_date = INCOMING_DATE_RX.search(line)
        if not m_date:
            i += 1
            continue

        published = m_date.group(1)

        # Uppdragsnummer might be on same line or next lines
        assignment_no = ""
        # check current + next 3 lines
        for k in range(0, 4):
            if i + k >= len(lines):
                break
            m_no = ASSIGNMENT_NO_RX.search(lines[i + k])
            if m_no:
                assignment_no = m_no.group(1)
                break

        if not assignment_no:
            i += 1
            continue

        # Title = scan backwards for nearest probable title
        title = ""
        for back in range(1, 8):
            idx = i - back
            if idx < 0:
                break
            cand = lines[idx]
            if _is_probable_title(cand):
                title = cand
                break

        if not title:
            # fallback: if we can't find title, skip (or set generic)
            i += 1
            continue

        # Build a "block" for location inference: from title down a bit
        block_lines = [title]
        for j in range(i, min(i + 20, len(lines))):
            block_lines.append(lines[j])
        block_text = "\n".join(block_lines)
        location = _infer_location_from_block(block_text)

        job_url = f"{canon_base}#uppdrag-{assignment_no}"

        results.append({
            "id": f"biolit-{assignment_no}",  # stable
            "title": title,
            "company": "Biolit",
            "location": location,
            "published": published,
            "url": job_url
        })

        i += 1

    return results