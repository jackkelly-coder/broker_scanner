# quality.py
import re
from typing import Dict, Tuple

_COOKIE_RX = re.compile(r"(välj vilka cookies|choose which cookies|cookie|cookies)", re.IGNORECASE)
_PAGER_RX = re.compile(r"^\s*(\d+|»|«|›|‹)\s*$")
_TOO_SHORT_RX = re.compile(r"^\s*[A-Za-zÅÄÖåäö]{1,2}\s*$")

def validate_assignment(a: Dict) -> Tuple[bool, str]:
    """
    Returns (ok, reason). Keep reasons stable so we can count them.
    """
    title = (a.get("title") or "").strip()
    url = (a.get("url") or "").strip()
    company = (a.get("company") or "").strip()

    if not company:
        return False, "missing_company"
    if not title:
        return False, "missing_title"
    if not url or not url.startswith("http"):
        return False, "bad_url"

    if _COOKIE_RX.search(title):
        return False, "cookie_title"
    if _PAGER_RX.match(title):
        return False, "pager_title"
    if len(title) < 4 or _TOO_SHORT_RX.match(title):
        return False, "too_short_title"

    # Common junk patterns
    if title.lower() in {"taxonomy", "taxonomin"}:
        return False, "taxonomy_title"

    return True, "ok"