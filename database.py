import os
import sqlite3
from datetime import datetime, timezone
import hashlib

import config

from geo import compute_location_bucket
from utils import canonicalize_url, clean_text

BASE_DIR = os.path.dirname(__file__)

# If DATABASE_PATH is relative, keep it relative to project root.
# If it's absolute, use it as-is.
DB_NAME = (
    config.DATABASE_PATH
    if os.path.isabs(config.DATABASE_PATH)
    else os.path.join(BASE_DIR, config.DATABASE_PATH)
)

# ----------------------------
# Company normalization
# ----------------------------
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
    c = clean_text(company or "")
    if not c:
        return ""
    key = c.lower()
    return COMPANY_ALIASES.get(key, c)


# ----------------------------
# SQLite connection
# ----------------------------
def _connect():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cursor.fetchall()]
    return column in cols


def _dedup_existing_rows(conn: sqlite3.Connection) -> int:
    """
    Removes duplicates by (company,url) keeping the newest scraped_at.
    Returns number of deleted rows.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT company, url, COUNT(*)
        FROM assignments
        WHERE company IS NOT NULL AND url IS NOT NULL AND url != ''
        GROUP BY company, url
        HAVING COUNT(*) > 1
    """)
    dups = cur.fetchall()
    if not dups:
        return 0

    deleted = 0
    for company, url, _cnt in dups:
        cur.execute("""
            SELECT id, scraped_at
            FROM assignments
            WHERE company = ? AND url = ?
            ORDER BY scraped_at DESC
        """, (company, url))
        rows = cur.fetchall()
        keep_id = rows[0][0]
        for rid, _ in rows[1:]:
            cur.execute("DELETE FROM assignments WHERE id = ?", (rid,))
            deleted += 1

    conn.commit()
    return deleted


def init_db():
    conn = _connect()
    cursor = conn.cursor()

    cursor.execute("""
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
    """)

    if not _column_exists(cursor, "assignments", "location_bucket"):
        cursor.execute("ALTER TABLE assignments ADD COLUMN location_bucket TEXT")

    cursor.execute("SELECT id, title, location, location_bucket FROM assignments")
    rows = cursor.fetchall()
    for assignment_id, title, location, bucket in rows:
        if bucket:
            continue
        b = compute_location_bucket(location or "", title or "")
        cursor.execute(
            "UPDATE assignments SET location_bucket=? WHERE id=?",
            (b, assignment_id),
        )

    deleted = _dedup_existing_rows(conn)
    if deleted:
        print(f"[db] Dedup migration: deleted {deleted} duplicate rows (company,url)")

    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_assignments_company_url ON assignments(company, url)"
    )

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assignments_company ON assignments(company)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assignments_location ON assignments(location)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assignments_location_bucket ON assignments(location_bucket)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_assignments_scraped_at ON assignments(scraped_at)")

    conn.commit()
    conn.close()


def _stable_id(company: str, url: str) -> str:
    key = f"{company}|{url}".encode("utf-8")
    return hashlib.md5(key).hexdigest()


def save_assignments(assignments):
    """
    Upsert assignments with DB-level dedup on (company,url).
    - Canonicalizes url centrally
    - Normalizes company centrally
    - Computes location_bucket centrally
    - Stores scraped_at as UTC ISO
    """
    if not assignments:
        return

    conn = _connect()
    cursor = conn.cursor()

    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for a in assignments:
        if not isinstance(a, dict):
            continue

        title = clean_text(a.get("title") or "")
        company_raw = clean_text(a.get("company") or "")
        company = normalize_company(company_raw)

        location = clean_text(a.get("location") or "")
        published = clean_text(a.get("published") or "")
        url_raw = clean_text(a.get("url") or "")

        if not title or not company or not url_raw:
            continue

        url = canonicalize_url(url_raw)

        if company == "Tingent" and title.strip('"').lower() == "jobs":
            continue
        if company == "A Society" and title.lower() == "english":
            continue

        location_bucket = compute_location_bucket(location, title)
        assignment_id = _stable_id(company, url)

        cursor.execute("""
            INSERT INTO assignments (
                id, title, company, location, location_bucket, published, url, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company, url) DO UPDATE SET
                title=excluded.title,
                location=excluded.location,
                location_bucket=excluded.location_bucket,
                published=excluded.published,
                scraped_at=excluded.scraped_at,
                id=excluded.id
        """, (
            assignment_id,
            title,
            company,
            location,
            location_bucket,
            published,
            url,
            now_utc,
        ))

    conn.commit()
    conn.close()