import hashlib
import os
import pyodbc
from datetime import datetime, timezone
from typing import Iterable

import config
from geo import compute_location_bucket
from utils import canonicalize_url, clean_text


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


def _connect():
    conn_str = (
        f"DRIVER={{{config.SQL_DRIVER}}};"
        f"SERVER={config.SQL_SERVER};"
        f"DATABASE={config.SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def _column_exists(cursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
        """,
        (table, column),
    )
    return cursor.fetchone() is not None


def _stable_id(company: str, url: str) -> str:
    key = f"{company}|{url}".encode("utf-8")
    return hashlib.md5(key).hexdigest()

def _dedup_existing_rows(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT company, url, COUNT(*)
        FROM dbo.assignments
        WHERE company IS NOT NULL
          AND url IS NOT NULL
          AND url <> ''
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
            FROM dbo.assignments
            WHERE company = ? AND url = ?
            ORDER BY scraped_at DESC
            """,
            (company, url),
        )
        rows = cur.fetchall()
        for row_id, _ in rows[1:]:
            cur.execute("DELETE FROM dbo.assignments WHERE id = ?", (row_id,))
            deleted += 1

    conn.commit()
    return deleted



def init_db() -> None:
    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        """
        IF OBJECT_ID('dbo.assignments', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.assignments (
                id NVARCHAR(36) NOT NULL PRIMARY KEY,
                title NVARCHAR(500) NOT NULL,
                company NVARCHAR(255) NOT NULL,
                location NVARCHAR(255) NULL,
                location_bucket NVARCHAR(255) NULL,
                published NVARCHAR(100) NULL,
                url NVARCHAR(1000) NOT NULL,
                scraped_at NVARCHAR(50) NOT NULL,
                first_scraped_at NVARCHAR(50) NULL
            )
        END
        """
    )

    if not _column_exists(cur, "assignments", "location_bucket"):
        cur.execute("ALTER TABLE dbo.assignments ADD location_bucket NVARCHAR(255) NULL")

    if not _column_exists(cur, "assignments", "first_scraped_at"):
        cur.execute("ALTER TABLE dbo.assignments ADD first_scraped_at NVARCHAR(50) NULL")

    cur.execute("SELECT id, title, location, location_bucket FROM dbo.assignments")
    for assignment_id, title, location, bucket in cur.fetchall():
        if bucket:
            continue
        cur.execute(
            "UPDATE dbo.assignments SET location_bucket = ? WHERE id = ?",
            (compute_location_bucket(location or "", title or ""), assignment_id),
        )

    cur.execute(
        """
        UPDATE dbo.assignments
        SET first_scraped_at = scraped_at
        WHERE first_scraped_at IS NULL
        """
    )

    _dedup_existing_rows(conn)

    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE name = 'ux_assignments_url'
              AND object_id = OBJECT_ID('dbo.assignments')
        )
        CREATE UNIQUE INDEX ux_assignments_url
        ON dbo.assignments(url)
        """
    )

    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'idx_assignments_company'
              AND object_id = OBJECT_ID('dbo.assignments')
        )
        CREATE INDEX idx_assignments_company
        ON dbo.assignments(company)
        """
    )

    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'idx_assignments_location'
              AND object_id = OBJECT_ID('dbo.assignments')
        )
        CREATE INDEX idx_assignments_location
        ON dbo.assignments(location)
        """
    )

    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'idx_assignments_location_bucket'
              AND object_id = OBJECT_ID('dbo.assignments')
        )
        CREATE INDEX idx_assignments_location_bucket
        ON dbo.assignments(location_bucket)
        """
    )

    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'idx_assignments_scraped_at'
              AND object_id = OBJECT_ID('dbo.assignments')
        )
        CREATE INDEX idx_assignments_scraped_at
        ON dbo.assignments(scraped_at)
        """
    )

    cur.execute(
        """
        IF OBJECT_ID('dbo.broker_scrape_logs', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.broker_scrape_logs (
                broker_name NVARCHAR(255) NOT NULL PRIMARY KEY,
                last_scrape_status NVARCHAR(50) NOT NULL,
                last_scrape_timestamp NVARCHAR(50) NOT NULL,
                last_error_message NVARCHAR(MAX) NULL
            )
        END
        """
    )

    conn.commit()
    conn.close()

def save_assignments(assignments: Iterable[dict]) -> dict:
    raw_items = list(assignments or [])
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    if not raw_items:
        return {
            "input": 0,
            "valid_rows": 0,
            "batch_duplicates": 0,
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
            "skipped": 0,
        }

    prepared_rows = []
    skipped = 0

    for item in raw_items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        title = clean_text(item.get("title") or "")
        company = normalize_company(item.get("company") or "")
        location = normalize_location(item.get("location") or "")
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

        location_bucket = normalize_location_bucket(item.get("location") or "")
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
                "first_scraped_at": now_utc,
            }
        )

    valid_rows = len(prepared_rows)

    # Deduplicera på url
    deduped_by_url: dict[str, dict] = {}
    for row in prepared_rows:
        deduped_by_url[row["url"]] = row

    deduped_rows = list(deduped_by_url.values())
    batch_duplicates = valid_rows - len(deduped_rows)

    conn = _connect()
    cur = conn.cursor()

    inserted = 0
    updated = 0
    deleted = 0

    incoming_urls = {row["url"] for row in deduped_rows}

    # Hämta befintliga URL:er
    cur.execute("SELECT url, first_scraped_at FROM dbo.assignments")
    existing = {row[0]: row[1] for row in cur.fetchall()}

    for row in deduped_rows:
        existing_first_scraped_at = existing.get(row["url"])

        if existing_first_scraped_at:
            cur.execute(
                """
                UPDATE dbo.assignments
                SET
                    id = ?,
                    title = ?,
                    company = ?,
                    location = ?,
                    location_bucket = ?,
                    published = ?,
                    scraped_at = ?
                WHERE url = ?
                """,
                (
                    row["id"],
                    row["title"],
                    row["company"],
                    row["location"],
                    row["location_bucket"],
                    row["published"],
                    row["scraped_at"],
                    row["url"],
                ),
            )
            updated += 1
        else:
            cur.execute(
                """
                INSERT INTO dbo.assignments (
                    id, title, company, location, location_bucket,
                    published, url, scraped_at, first_scraped_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    row["first_scraped_at"],
                ),
            )
            inserted += 1

    # Radera poster som inte längre finns i senaste hämtningen
    # if deduped_rows:
    #     urls_to_delete = set(existing.keys()) - incoming_urls
    #     for url in urls_to_delete:
    #         cur.execute("DELETE FROM dbo.assignments WHERE url = ?", (url,))
    #         deleted += 1

    conn.commit()
    conn.close()

    return {
        "input": len(raw_items),
        "valid_rows": valid_rows,
        "batch_duplicates": batch_duplicates,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "skipped": skipped,
    }

def sync_assignments(current_urls: set[str] | list[str]) -> int:
    urls = {u for u in (current_urls or []) if u}
    if not urls:
        return 0

    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT url FROM dbo.assignments")
    existing_urls = {row[0] for row in cur.fetchall() if row[0]}

    urls_to_delete = existing_urls - urls
    deleted = 0

    for url in urls_to_delete:
        cur.execute("DELETE FROM dbo.assignments WHERE url = ?", (url,))
        deleted += 1

    conn.commit()
    conn.close()
    return deleted

def log_scraper_result(broker_name: str, status: str, timestamp: str, error: str = "") -> None:
    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE dbo.broker_scrape_logs
        SET
            last_scrape_status = ?,
            last_scrape_timestamp = ?,
            last_error_message = CASE WHEN ? = 'ok' THEN NULL ELSE ? END
        WHERE broker_name = ?
        """,
        (status, timestamp, status, error, broker_name),
    )

    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO dbo.broker_scrape_logs
                (broker_name, last_scrape_status, last_scrape_timestamp, last_error_message)
            VALUES
                (?, ?, ?, CASE WHEN ? = 'ok' THEN NULL ELSE ? END)
            """,
            (broker_name, status, timestamp, status, error),
        )

    conn.commit()
    conn.close()

def normalize_location(location: str) -> str:
    value = clean_text(location or "")
    if not value:
        return ""

    value = value.replace("Sweden", "")
    value = value.replace("Sverige", "")
    value = value.replace(",", " ")
    value = value.strip()

    if not value:
        return ""

    return value.split()[0].strip(" ,")

def normalize_location_bucket(location: str) -> str:
    value = clean_text(location or "")
    if not value:
        return ""

    value = value.replace(",", " ")
    value = value.strip()

    if not value:
        return ""

    return value.split()[0]