# geo.py
from __future__ import annotations
import re

_ws = re.compile(r"\s+")
_word = lambda s: rf"(?:^|[\s,;/()\-]){re.escape(s)}(?:$|[\s,;/()\-])"

def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = _ws.sub(" ", s)
    return s

def _strip_country_suffix(s: str) -> str:
    """
    Normalize things like:
      - "Stockholm, SE" -> "Stockholm"
      - "Ängelholm, SE" -> "Ängelholm"
      - "Göteborg, Sweden" -> "Göteborg"
      - "SE" -> "Sweden" (handled elsewhere)
    """
    s2 = (s or "").strip()
    if not s2:
        return ""

    # remove trailing country tokens
    s2 = re.sub(r"\s*,\s*(se|swe|sweden|sverige)\s*$", "", s2, flags=re.IGNORECASE).strip()
    # remove "(SE)" etc
    s2 = re.sub(r"\s*\(\s*(se|swe|sweden|sverige)\s*\)\s*$", "", s2, flags=re.IGNORECASE).strip()

    return s2

def _normalize_location_phrase(location: str) -> str:
    """
    Normalize common noisy variants before classification.
    """
    s = (location or "").strip()
    if not s:
        return ""

    # If it's exactly SE -> Sweden
    if s.strip().lower() in {"se", "swe"}:
        return "Sweden"

    s = _strip_country_suffix(s)

    # Normalize metropolitan variants
    # "Stockholm Metropolitan Area" -> "Stockholm"
    s = re.sub(r"\bmetropolitan area\b", "", s, flags=re.IGNORECASE).strip()

    # Collapse whitespace
    s = " ".join(s.split())
    return s


SWEDISH_CITIES = {
    "stockholm", "solna", "sundbyberg", "kista", "bromma", "lidingö", "lidingo",
    "göteborg", "goteborg", "gothenburg",
    "malmö", "malmo", "lund", "helsingborg",
    "uppsala",
    "västerås", "vasteras",
    "örebro", "orebro",
    "linköping", "linkoping", "norrköping", "norrkoping",
    "jönköping", "jonkoping",
    "umeå", "umea",
    "sundsvall",
    "borås", "boras",
    "gävle", "gavle",
    "skövde", "skovde",
    "visby", "ängelholm", "angelholm",
}

SWEDEN_SIGNALS = {"sweden", "sverige", "se", "swe"}

EU_GENERIC = ("europe", "eu", "nordics", "scandinavia", "emea", "global", "worldwide", "international")

FOREIGN_HINTS = {
    "poland", "warsaw",
    "germany", "berlin", "munich", "münchen", "düsseldorf", "dusseldorf", "stuttgart",
    "france", "paris", "île-de-france", "ile-de-france",
    "romania", "bucharest",
    "portugal", "lisbon",
    "india", "pune", "bengaluru", "bangalore",
    "uk", "england", "london",
    "ireland", "dublin",
    "denmark", "copenhagen",
    "norway", "oslo",
    "finland", "helsinki",
}

_foreign_rx = re.compile("|".join(sorted((_word(x) for x in FOREIGN_HINTS), key=len, reverse=True)))
_sweden_rx = re.compile("|".join(sorted((_word(x) for x in SWEDEN_SIGNALS), key=len, reverse=True)))
_city_rx = re.compile("|".join(sorted((_word(x) for x in SWEDISH_CITIES), key=len, reverse=True)))


def is_sweden_assignment(location: str, title: str = "") -> bool:
    """
    Sweden-only filter.
    """
    loc_norm = _normalize_location_phrase(location)
    hay = norm_text(f"{loc_norm} {title}")

    if not hay.strip():
        return False

    if _foreign_rx.search(hay):
        return False

    sweden_hit = bool(_sweden_rx.search(hay) or _city_rx.search(hay))
    if not sweden_hit and any(x in hay for x in EU_GENERIC):
        return False

    return sweden_hit


STOCKHOLM_METRO = {
    "stockholm", "solna", "sundbyberg", "södertälje", "sodertalje", "nacka",
    "tyresö", "tyreso", "huddinge", "botkyrka", "jarfalla", "järfälla",
    "spånga", "kista", "bromma", "lidingö", "lidingo", "täby", "taby",
    "danderyd", "sollentuna", "vallentuna", "haninge", "upplands väsby",
    "upplands vasby", "upplands-bro", "upplands bro", "sigtuna",
    "stockholm city", "stockholm north",
}

_stockholm_rx = re.compile("|".join(sorted((_word(x) for x in STOCKHOLM_METRO), key=len, reverse=True)))

def compute_location_bucket(location: str, title: str = "") -> str:
    loc_norm = _normalize_location_phrase(location)
    hay = norm_text(f"{loc_norm} {title}")

    # Remote first (but still bucket if specific city is present)
    remote_hit = any(x in hay for x in (
        "remote", "distans", "på distans", "pa distans", "distansarbete",
        "hybrid", "work from home", "wfh"
    ))

    # City buckets override "Remote"
    if _stockholm_rx.search(hay):
        return "Stockholm"
    if any(x in hay for x in ("göteborg", "goteborg", "gothenburg")):
        return "Göteborg"
    if any(x in hay for x in ("malmö", "malmo", "lund", "helsingborg")):
        return "Malmö"
    if "uppsala" in hay:
        return "Uppsala"

    if remote_hit:
        return "Remote"

    if is_sweden_assignment(loc_norm, title):
        return "Sweden"

    if hay.strip():
        return "Other"

    return "Unknown"