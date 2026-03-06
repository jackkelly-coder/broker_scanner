import requests

from utils import clean_text, normalize_url


API_URL = "https://tingent.se/api/jobs"


def _extract_items(data) -> list:
    """
    Tingent API can return either a list or a dict containing a list.
    Normalize to a list of dict items.
    """
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        for key in ("jobs", "data", "results", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        for value in data.values():
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []


def _extract_source_id(item: dict) -> str:
    for key in (
        "abstract_id",
        "id",
        "job_id",
        "jobId",
        "requisition_id",
        "requisitionId",
        "uuid",
        "slug",
    ):
        value = item.get(key)
        if value is not None:
            value = clean_text(str(value))
            if value:
                return value
    return ""


def _extract_title(item: dict) -> str:
    return clean_text(
        str(
            item.get("requisition_name")
            or item.get("title")
            or item.get("name")
            or item.get("role")
            or ""
        )
    )


def _extract_location(item: dict) -> str:
    return clean_text(
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


def _extract_published(item: dict) -> str:
    return clean_text(
        str(
            item.get("published")
            or item.get("published_at")
            or item.get("created")
            or item.get("created_at")
            or ""
        )
    )


def _build_job_url(item: dict, listing_url: str, source_id: str) -> str:
    """
    Try to use a real job URL if the API provides one.
    If not, create a stable unique fallback based on the listing URL.
    """
    candidates = [
        item.get("url"),
        item.get("publicUrl"),
        item.get("public_url"),
        item.get("job_url"),
        item.get("jobUrl"),
        item.get("absolute_url"),
        item.get("absoluteUrl"),
        item.get("permalink"),
        item.get("link"),
        item.get("href"),
        item.get("path"),
        item.get("slug"),
    ]

    for candidate in candidates:
        if candidate is None:
            continue

        candidate = clean_text(str(candidate))
        if not candidate:
            continue

        # Absolute URL
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate

        # Relative path or slug
        if candidate.startswith("/"):
            return normalize_url("https://tingent.se", candidate)

        # Slug-like fallback
        if "/" not in candidate and " " not in candidate and len(candidate) < 120:
            return normalize_url("https://tingent.se", f"/jobs/{candidate}")

    # Stable unique fallback. Even if Tingent ignores the query string,
    # it still lands on the right site and remains unique for dedupe.
    separator = "&" if "?" in listing_url else "?"
    return f"{listing_url}{separator}job_id={source_id}"


def fetch(url: str):
    """
    Tingent: fetch via their public API.

    Critical fix:
    Never fall back to the exact same listing URL for every item.
    If the API lacks a real detail URL, we generate a stable unique URL
    using the source id.
    """
    results = []

    try:
        response = requests.get(API_URL, timeout=20)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        print(f"Error fetching {API_URL}: {exc}")
        return results

    items = _extract_items(data)
    seen_source_ids = set()

    for item in items:
        title = _extract_title(item)
        if not title or len(title) < 4:
            continue

        if title.strip().lower() in {"jobs", "job", "english"}:
            continue

        source_id = _extract_source_id(item)
        if not source_id:
            continue

        if source_id in seen_source_ids:
            continue
        seen_source_ids.add(source_id)

        location = _extract_location(item)
        published = _extract_published(item)
        job_url = _build_job_url(item, url, source_id)

        results.append(
            {
                "id": f"tingent-{source_id}",
                "title": title,
                "company": "Tingent",
                "location": location,
                "published": published,
                "url": job_url,
            }
        )

    return results