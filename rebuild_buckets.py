# rebuild_buckets.py
import sqlite3
from database import DB_NAME
from geo import compute_location_bucket

def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Hämta alla rader vi vill uppdatera
    cur.execute("""
        SELECT id, title, location, location_bucket
        FROM assignments
    """)
    rows = cur.fetchall()

    updated = 0
    unchanged = 0
    missing_loc = 0

    for assignment_id, title, location, old_bucket in rows:
        title = title or ""
        location = location or ""

        if not location.strip():
            missing_loc += 1

        new_bucket = compute_location_bucket(location, title)

        if (old_bucket or "") != (new_bucket or ""):
            cur.execute(
                "UPDATE assignments SET location_bucket = ? WHERE id = ?",
                (new_bucket, assignment_id)
            )
            updated += 1
        else:
            unchanged += 1

    conn.commit()
    conn.close()

    print("Backfill done.")
    print(f"Total rows: {len(rows)}")
    print(f"Updated buckets: {updated}")
    print(f"Unchanged: {unchanged}")
    print(f"Rows with empty location: {missing_loc}")

if __name__ == "__main__":
    main()