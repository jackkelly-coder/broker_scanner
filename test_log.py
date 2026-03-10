#!/usr/bin/env python3

from datetime import datetime, timezone
from database import init_db, log_scraper_result

# Initiera databasen (skapar tabellen om den inte finns)
init_db()

# Logga några testresultat
timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

log_scraper_result("test_broker", "ok", timestamp)
log_scraper_result("test_broker2", "error", timestamp, "Some error occurred")
log_scraper_result("test_broker", "timeout", timestamp, "Timed out")

print("Test logging completed.")