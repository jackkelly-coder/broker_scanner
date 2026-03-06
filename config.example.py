import os
import multiprocessing as mp

DATABASE_PATH = os.getenv("DATABASE_PATH", "database.db")
LOCATION_FILTER = os.getenv("LOCATION_FILTER", "<city>").strip()

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
HTTP_WORKERS = int(os.getenv("HTTP_WORKERS", str(MAX_WORKERS)))
BROWSER_WORKERS = int(os.getenv("BROWSER_WORKERS", "1"))
SCRAPER_TIMEOUT_S = int(os.getenv("SCRAPER_TIMEOUT_S", "300"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

SWEDEN_ONLY = os.getenv("SWEDEN_ONLY", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

USE_SUBPROCESS = os.getenv("USE_SUBPROCESS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
    "on",
)

MP_CTX = mp.get_context("spawn")