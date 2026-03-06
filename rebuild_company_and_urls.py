# rebuild_company_and_urls.py
import sqlite3

from database import DB_NAME, normalize_company, _stable_id
from utils import canonicalize_url, clean_text
from geo import compute_location_bucket


def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # WAL mode is fine during migration too
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # Read all existing rows
    cur.execute("""
        SELECT id, title, company, location, location_bucket, published, url, scraped_at
        FROM assignments
    """)
    rows = cur.fetchall()

    # Create staging table
    cur.execute("DROP TABLE IF EXISTS assignments_new")
    cur.execute("""
        CREATE TABLE assignments_new (
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

    # Enforce unique on staging table as well
    cur.execute("CREATE UNIQUE INDEX ux_assignments_new_company_url ON assignments_new(company, url)")
    cur.execute("CREATE INDEX idx_assignments_new_scraped_at ON assignments_new(scraped_at)")

    inserted = 0
    updated = 0

    for (_rid, title, company, location, _bucket, published, url, scraped_at) in rows:
        title = clean_text(title or "")
        company_old = clean_text(company or "")
        location = clean_text(location or "")
        published = clean_text(published or "")
        url_old = clean_text(url or "")
        scraped_at = clean_text(scraped_at or "")

        company_new = normalize_company(company_old)
        url_new = canonicalize_url(url_old)
        bucket_new = compute_location_bucket(location, title)
        new_id = _stable_id(company_new, url_new)

        # Insert / upsert keeping the newest scraped_at
        # If conflict, update only if incoming scraped_at is newer/equal
        cur.execute("""
            INSERT INTO assignments_new (id, title, company, location, location_bucket, published, url, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company, url) DO UPDATE SET
                id = CASE
                        WHEN excluded.scraped_at >= assignments_new.scraped_at THEN excluded.id
                        ELSE assignments_new.id
                     END,
                title = CASE
                        WHEN excluded.scraped_at >= assignments_new.scraped_at THEN excluded.title
                        ELSE assignments_new.title
                     END,
                location = CASE
                        WHEN excluded.scraped_at >= assignments_new.scraped_at THEN excluded.location
                        ELSE assignments_new.location
                     END,
                location_bucket = CASE
                        WHEN excluded.scraped_at >= assignments_new.scraped_at THEN excluded.location_bucket
                        ELSE assignments_new.location_bucket
                     END,
                published = CASE
                        WHEN excluded.scraped_at >= assignments_new.scraped_at THEN excluded.published
                        ELSE assignments_new.published
                     END,
                scraped_at = CASE
                        WHEN excluded.scraped_at >= assignments_new.scraped_at THEN excluded.scraped_at
                        ELSE assignments_new.scraped_at
                     END
        """, (new_id, title, company_new, location, bucket_new, published, url_new, scraped_at))

        # rowcount in sqlite is tricky for upsert; track stats roughly:
        inserted += 1

    conn.commit()

    # Count dedup result
    cur.execute("SELECT COUNT(*) FROM assignments")
    old_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM assignments_new")
    new_count = cur.fetchone()[0]

    # Swap tables atomically
    cur.execute("DROP TABLE IF EXISTS assignments_old")
    cur.execute("ALTER TABLE assignments RENAME TO assignments_old")
    cur.execute("ALTER TABLE assignments_new RENAME TO assignments")

    # Recreate indexes on new main table
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_assignments_company_url ON assignments(company, url)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_company ON assignments(company)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_location ON assignments(location)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_location_bucket ON assignments(location_bucket)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_assignments_scraped_at ON assignments(scraped_at)")

    # Drop old table
    cur.execute("DROP TABLE assignments_old")

    conn.commit()
    conn.close()

    print("Company+URL rebuild done (staging migration).")
    print(f"Old row count: {old_count}")
    print(f"New row count (after normalization+dedup): {new_count}")
    print(f"Rows collapsed (dedup gain): {old_count - new_count}")


if __name__ == "__main__":
    main()