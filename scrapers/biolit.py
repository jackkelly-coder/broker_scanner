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

    match = PLACE_RX.search(block_text)
    if match:
        location = clean_text(match.group(1))
        return location[:80]

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
    Heuristic: titles are usually short-ish, not boilerplate,
    and should not look like metadata lines.
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

    if len(line) > 90:
        return False

    if "@" in line or "070" in line or "073" in line or "08-" in line:
        return False

    return any(ch.isalpha() for ch in line)


def fetch(url: str):
    """
    Biolit is a single long page.

    Important:
    We must NOT use #fragment-based URLs because canonicalize_url()
    removes fragments. Instead we create a stable query-param URL
    per assignment number.
    """
    results = []

    try:
        response = requests.get(
            url,
            timeout=20,
            headers={"Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"},
        )
        response.raise_for_status()
    except Exception as exc:
        print(f"Error fetching {url}: {exc}")
        return results

    soup = BeautifulSoup(response.text, "html.parser")

    for br in soup.find_all("br"):
        br.replace_with("\n")

    raw_text = soup.get_text("\n")
    lines = [clean_text(x) for x in raw_text.split("\n")]
    lines = [x for x in lines if x]

    canon_base = canonicalize_url(url)

    index = 0
    seen_assignment_numbers = set()

    while index < len(lines):
        line = lines[index]
        date_match = INCOMING_DATE_RX.search(line)
        if not date_match:
            index += 1
            continue

        published = date_match.group(1)

        assignment_no = ""
        for offset in range(0, 4):
            current_index = index + offset
            if current_index >= len(lines):
                break

            number_match = ASSIGNMENT_NO_RX.search(lines[current_index])
            if number_match:
                assignment_no = number_match.group(1)
                break

        if not assignment_no:
            index += 1
            continue

        if assignment_no in seen_assignment_numbers:
            index += 1
            continue
        seen_assignment_numbers.add(assignment_no)

        title = ""
        for back in range(1, 8):
            title_index = index - back
            if title_index < 0:
                break

            candidate = lines[title_index]
            if _is_probable_title(candidate):
                title = candidate
                break

        if not title:
            index += 1
            continue

        block_lines = [title]
        for j in range(index, min(index + 20, len(lines))):
            block_lines.append(lines[j])

        block_text = "\n".join(block_lines)
        location = _infer_location_from_block(block_text)

        # Use query param instead of fragment so canonicalize_url keeps uniqueness.
        job_url = f"{canon_base}?assignment_no={assignment_no}"

        results.append(
            {
                "id": f"biolit-{assignment_no}",
                "title": title,
                "company": "Biolit",
                "location": location,
                "published": published,
                "url": job_url,
            }
        )

        index += 1

    return results