# export.py
import os
import csv
import sqlite3
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from database import DB_NAME
import app_config

XLSX_PATH = os.path.join(app_config.EXPORT_DIR, "assignments.xlsx")
CSV_PATH = os.path.join(app_config.EXPORT_DIR, "assignments.csv")


def fetch_assignments():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # DB-enforced UNIQUE(company,url) => ingen dedup behövs i export.
    cursor.execute("""
        SELECT
            title,
            company,
            location,
            location_bucket,
            published,
            url,
            scraped_at
        FROM assignments
        ORDER BY scraped_at DESC
    """)

    rows = cursor.fetchall()
    conn.close()

    headers = [
        "title",
        "company",
        "location_raw",
        "location_bucket",
        "published",
        "url",
        "scraped_at",
    ]
    return headers, rows


def export_csv(headers, rows):
    os.makedirs(app_config.EXPORT_DIR, exist_ok=True)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"Wrote CSV: {CSV_PATH}")


def export_xlsx(headers, rows):
    os.makedirs(app_config.EXPORT_DIR, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "assignments"

    ws.append(headers)

    # Freeze header + filter
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"

    url_col_index = headers.index("url") + 1  # 1-based

    for row_idx, row in enumerate(rows, start=2):
        ws.append(list(row))
        url_value = row[url_col_index - 1]
        if url_value:
            cell = ws.cell(row=row_idx, column=url_col_index)
            cell.hyperlink = url_value
            cell.value = url_value
            cell.style = "Hyperlink"

    # Autosize columns (cap)
    for col_idx in range(1, len(headers) + 1):
        max_len = 0
        # Cap scanning for speed
        for r in ws.iter_rows(min_row=1, max_row=min(ws.max_row, 2000), min_col=col_idx, max_col=col_idx):
            v = r[0].value
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 2, 60)

    wb.save(XLSX_PATH)
    print(f"Wrote XLSX: {XLSX_PATH}")


def export_all():
    headers, rows = fetch_assignments()
    export_csv(headers, rows)
    export_xlsx(headers, rows)
    print(f"Export complete. Rows: {len(rows)}")


if __name__ == "__main__":
    export_all()