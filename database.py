import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

import config
from geo import compute_location_bucket
from utils import canonicalize_url, clean_text

BASE_DIR = os.path.dirname(__file__)
DB_NAME = (
    config.DATABASE_PATH
    if os.path.isabs(config.DATABASE_PATH)
    else os.path.join(BASE_DIR, config.DATABASE_PATH)
)

COMPANY_ALIASES = {
    "e-work": "Ework",
    "e work": "Ework",
    "ework": "Ework",
    "e-work group": "Ework",
    "emagine": "emagine",
    "tingent": "Tingent",
    "a society": "A Society",
    "bohmans": "Bohmans",
    "digitalenta": "Digitalenta",
    "enkl": "Enkl",
    "upgraded": "Upgraded",
    "biolit": "Biolit",
    "headagent": "HeadAgent",
    "teksystems": "Teksystems",
    "verama": "Verama",
}


def normalize_company(company: str) -> str:
    cleaned = clean_text(company or "")
    if not cleaned:
        return ""
    return COMPANY_ALIASES.get(cleaned.lower(), cleaned)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _column_exists(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cursor.fetchall())


def _stable_id(company: str, url: str) -> str:
    key = f"{company}|{url}".encode("utf-8")
    return hashlib.md5(key).hexdigest()


def _dedup_existing_rows(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT company, url, COUNT(*)
        FROM assignments
        WHERE company IS NOT NULL
          AND url IS NOT NULL
          AND url != ''
        GROUP BY company, url
        HAVING COUNT(*) > 1
        """
    )
    duplicates = cur.fetchall()
    if not duplicates:
        return 0

    deleted = 0
    for company, url, _count in duplicates:
        cur.execute(
            """
            SELECT id, scraped_at
            FROM assignments
            WHERE company = ? AND url = ?
            ORDER BY scraped_at DESC
            """,
            (company, url),
        )
        rows = cur.fetchall()
        for row_id, _ in rows[1:]:
            cur.execute("DELETE FROM assignments WHERE id = ?", (row_id,))
            deleted += 1

    conn.commit()
    return deleted


def init_db() -> None:
    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS assignments (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            company TEXT NOT NULL,
            location TEXT,
            location_bucket TEXT,
            published TEXT,
            url TEXT NOT NULL,
            scraped_at TEXT NOT NULL
        )
        """
    )

    if not _column_exists(cur, "assignments", "location_bucket"):
        cur.execute("ALTER TABLE assignments ADD COLUMN location_bucket TEXT")

    cur.execute("SELECT id, title, location, location_bucket FROM assignments")
    for assignment_id, title, location, bucket in cur.fetchall():
        if bucket:
            continue
        cur.execute(
            "UPDATE assignments SET location_bucket = ? WHERE id = ?",
            (compute_location_bucket(location or "", title or ""), assignment_id),
        )

    _dedup_existing_rows(conn)

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_assignments_company_url ON assignments(company, url)"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_company ON assignments(company)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_location ON assignments(location)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_assignments_location_bucket ON assignments(location_bucket)"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_scraped_at ON assignments(scraped_at)")

    conn.commit()
    conn.close()


def save_assignments(assignments: Iterable[dict]) -> dict:
    """
    Saves assignments with:
    - in-memory batch dedupe on (company, canonical_url)
    - DB-level dedupe on (company, url)

    Returns stats:
    {
        "input": int,
        "valid_rows": int,
        "batch_duplicates": int,
        "inserted": int,
        "updated": int,
        "skipped": int,
    }
    """
    raw_items = list(assignments or [])
    if not raw_items:
        return {
            "input": 0,
            "valid_rows": 0,
            "batch_duplicates": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
        }

    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    prepared_rows = []
    skipped = 0

    for item in raw_items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        title = clean_text(item.get("title") or "")
        company = normalize_company(item.get("company") or "")
        location = clean_text(item.get("location") or "")
        published = clean_text(item.get("published") or "")
        raw_url = clean_text(item.get("url") or "")

        if not title or not company or not raw_url:
            skipped += 1
            continue

        url = canonicalize_url(raw_url)

        if company == "Tingent" and title.strip('"').lower() == "jobs":
            skipped += 1
            continue

        if company == "A Society" and title.lower() == "english":
            skipped += 1
            continue

        location_bucket = compute_location_bucket(location, title)
        assignment_id = _stable_id(company, url)

        prepared_rows.append(
            {
                "id": assignment_id,
                "title": title,
                "company": company,
                "location": location,
                "location_bucket": location_bucket,
                "published": published,
                "url": url,
                "scraped_at": now_utc,
            }
        )

    valid_rows = len(prepared_rows)

    # Batch dedupe before DB writes.
    deduped_by_key: dict[tuple[str, str], dict] = {}
    for row in prepared_rows:
        key = (row["company"], row["url"])
        deduped_by_key[key] = row

    deduped_rows = list(deduped_by_key.values())
    batch_duplicates = valid_rows - len(deduped_rows)

    conn = _connect()
    cur = conn.cursor()

    inserted = 0
    updated = 0

    for row in deduped_rows:
        cur.execute(
            "SELECT 1 FROM assignments WHERE company = ? AND url = ? LIMIT 1",
            (row["company"], row["url"]),
        )
        exists = cur.fetchone() is not None

        cur.execute(
            """
            INSERT INTO assignments (
                id, title, company, location, location_bucket, published, url, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company, url) DO UPDATE SET
                id = excluded.id,
                title = excluded.title,
                location = excluded.location,
                location_bucket = excluded.location_bucket,
                published = excluded.published,
                scraped_at = excluded.scraped_at
            """,
            (
                row["id"],
                row["title"],
                row["company"],
                row["location"],
                row["location_bucket"],
                row["published"],
                row["url"],
                row["scraped_at"],
            ),
        )

        if exists:
            updated += 1
        else:
            inserted += 1

    conn.commit()
    conn.close()

    return {
        "input": len(raw_items),
        "valid_rows": valid_rows,
        "batch_duplicates": batch_duplicates,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
    }