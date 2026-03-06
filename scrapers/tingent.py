import requests
from utils import generate_id, clean_text


API_URL = "https://tingent.se/api/jobs"


def _extract_items(data) -> list:
    """
    Tingent API can return either a list or a dict containing a list.
    We normalize to a list of dict items.
    """
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    if isinstance(data, dict):
        for key in ("jobs", "data", "results", "items"):
            v = data.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

        # last resort: first list value in dict
        for v in data.values():
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

    return []


def fetch(url: str):
    """
    Tingent: fetch via their public API (API first, no Playwright).
    Returns list of:
    {
        "id": str,
        "title": str,
        "company": str,
        "location": str,
        "published": str,
        "url": str
    }
    """
    results = []

    try:
        r = requests.get(API_URL, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Error fetching {API_URL}: {e}")
        return results

    items = _extract_items(data)
    seen_ids = set()

    for item in items:
        title = clean_text(
            str(
                item.get("requisition_name")
                or item.get("title")
                or item.get("name")
                or item.get("role")
                or ""
            )
        )
        if not title or len(title) < 4:
            continue
        if title.strip().lower() in {"jobs", "job", "english"}:
            continue

        # Prefer stable source id from API
        source_id = item.get("abstract_id") or item.get("id")
        if not source_id:
            continue
        source_id = str(source_id).strip()
        if not source_id or source_id in seen_ids:
            continue
        seen_ids.add(source_id)

        # Prefer explicit job URL if present. Do not guess paths (avoid hacks).
        job_url = (
            item.get("url")
            or item.get("publicUrl")
            or item.get("public_url")
            or ""
        )
        job_url = clean_text(str(job_url))
        if not job_url:
            # fallback to listing url passed into fetch()
            job_url = url

        location = clean_text(
            str(
                item.get("location_city")
                or item.get("location_name")
                or item.get("requisition_locationid")
                or item.get("location")
                or item.get("city")
                or item.get("office")
                or ""
            )
        )

        published = clean_text(
            str(
                item.get("published")
                or item.get("published_at")
                or item.get("created")
                or item.get("created_at")
                or ""
            )
        )

        # Deterministic namespaced id to avoid cross-company collisions
        # Use source_id since it's stable, and namespace it.
        stable_id = f"tingent-{source_id}"

        results.append(
            {
                "id": stable_id,
                "title": title,
                "company": "Tingent",
                "location": location,
                "published": published,
                "url": job_url,
            }
        )

    return results