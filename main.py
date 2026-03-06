import importlib
import logging
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
import traceback
from typing import Any
import threading

from geo import is_sweden_assignment

from app_config import SCRAPERS, OUTPUTS
from database import init_db, save_assignments
from export import export_all
from quality import validate_assignment

logger = logging.getLogger(__name__)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
SCRAPER_TIMEOUT_S = int(os.getenv("SCRAPER_TIMEOUT_S", "180"))

HTTP_WORKERS = int(os.getenv("HTTP_WORKERS", str(MAX_WORKERS)))
BROWSER_WORKERS = int(os.getenv("BROWSER_WORKERS", "1"))

SWEDEN_ONLY = os.getenv("SWEDEN_ONLY", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

USE_SUBPROCESS = os.getenv("USE_SUBPROCESS", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

# IMPORTANT:
# Use "spawn" context to avoid deadlocks when starting processes from worker threads.
MP_CTX = mp.get_context("spawn")


def _is_sweden_only(item: dict) -> bool:
    return is_sweden_assignment(item.get("location") or "", item.get("title") or "")


def _get_location_filter() -> str:
    for key in ("LOCATION_FILTER", "FILTER_LOCATION"):
        v = os.getenv(key, "").strip().lower()
        if v:
            return v
    return ""


def _matches_location_filter(item: dict, wanted: str) -> bool:
    loc = (item.get("location") or "").lower()
    title = (item.get("title") or "").lower()
    return wanted in loc or wanted in title


def _safe_len(x: Any) -> int:
    try:
        return len(x)
    except Exception:
        return 0


def _child_logging_init() -> None:
    level = (os.getenv("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _scraper_process_entry(conn, module_name: str, url: str) -> None:
    _child_logging_init()
    try:
        t0 = time.perf_counter()
        module = importlib.import_module(f"scrapers.{module_name}")
        results = module.fetch(url)
        dt = time.perf_counter() - t0

        if results is None:
            results = []
        if not isinstance(results, list):
            raise TypeError(f"Scraper {module_name}.fetch() must return list[dict], got {type(results)}")

        conn.send(("ok", module_name, float(dt), int(len(results)), results))
    except Exception:
        conn.send(("err", module_name, traceback.format_exc()))
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _run_scraper_with_timeout(module_name: str, url: str, timeout_s: int):
    """
    Hard timeout via subprocess (spawn).
    Returns: (module, fetch_s, found, results, status, err)
    status: ok | timeout | error
    """
    parent_conn, child_conn = MP_CTX.Pipe(duplex=False)
    p = MP_CTX.Process(
        target=_scraper_process_entry,
        args=(child_conn, module_name, url),
        daemon=True,
    )

    try:
        p.start()
    except Exception as e:
        try:
            parent_conn.close()
            child_conn.close()
        except Exception:
            pass
        return (module_name, 0.0, 0, [], "error", f"Failed to start subprocess: {e}")

    try:
        child_conn.close()
    except Exception:
        pass

    p.join(timeout_s)

    if p.is_alive():
        p.terminate()
        p.join(5)
        try:
            parent_conn.close()
        except Exception:
            pass
        return (module_name, 0.0, 0, [], "timeout", f"Timed out after {timeout_s}s")

    msg = None
    try:
        if parent_conn.poll(1.0):
            msg = parent_conn.recv()
    except Exception as e:
        try:
            parent_conn.close()
        except Exception:
            pass
        return (module_name, 0.0, 0, [], "error", f"Failed reading result from child: {e}")

    try:
        parent_conn.close()
    except Exception:
        pass

    if not msg:
        return (module_name, 0.0, 0, [], "error", "Scraper process exited without sending a result")

    if msg[0] == "ok":
        _, mod, dt, found, results = msg
        return (mod, float(dt), int(found), results, "ok", "")
    else:
        _, mod, tb = msg
        return (mod, 0.0, 0, [], "error", tb)


def _run_scraper_direct(module_name: str, url: str):
    """
    Runs inside current process/thread. Faster, but cannot be hard-killed.
    """
    try:
        t0 = time.perf_counter()
        module = importlib.import_module(f"scrapers.{module_name}")
        results = module.fetch(url)
        dt = time.perf_counter() - t0

        if results is None:
            results = []
        if not isinstance(results, list):
            raise TypeError(f"Scraper {module_name}.fetch() must return list[dict], got {type(results)}")

        return (module_name, float(dt), int(len(results)), results, "ok", "")
    except Exception:
        return (module_name, 0.0, 0, [], "error", traceback.format_exc())


def run():
    wanted = _get_location_filter()

    http_sem = threading.Semaphore(max(1, HTTP_WORKERS))
    browser_sem = threading.Semaphore(max(1, BROWSER_WORKERS))

    logger.info(
        "Starting run. Sweden-only=%s | Location filter='%s' | Scrapers=%s | "
        "MAX_WORKERS=%s HTTP_WORKERS=%s BROWSER_WORKERS=%s USE_SUBPROCESS=%s (mp=spawn)",
        SWEDEN_ONLY,
        wanted if wanted else "<none>",
        len(SCRAPERS),
        MAX_WORKERS,
        HTTP_WORKERS,
        BROWSER_WORKERS,
        USE_SUBPROCESS,
    )

    init_db()

    total_found = 0
    total_kept = 0

    def _run_gated(module_name: str, url: str, timeout_s: int, engine: str):
        sem = browser_sem if engine == "browser" else http_sem
        logger.info("Waiting slot: %s | engine=%s", module_name, engine)
        with sem:
            logger.info("Running: %s | engine=%s | timeout=%ss", module_name, engine, timeout_s)
            if USE_SUBPROCESS:
                return _run_scraper_with_timeout(module_name, url, timeout_s)
            return _run_scraper_direct(module_name, url)

    futures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for conf in SCRAPERS:
            module_name = conf["module"]
            url = conf["url"]
            timeout_s = int(conf.get("timeout_s", SCRAPER_TIMEOUT_S))
            engine = (conf.get("engine") or "http").strip().lower()

            logger.info("Queued: %s | engine=%s", module_name, engine)
            fut = pool.submit(_run_gated, module_name, url, timeout_s, engine)
            futures[fut] = {"module": module_name, "url": url, "timeout_s": timeout_s, "engine": engine}

        for fut in as_completed(futures):
            meta = futures[fut]
            module_name = meta["module"]
            url = meta["url"]
            timeout_s = meta["timeout_s"]
            engine = meta["engine"]

            try:
                mod, fetch_s, found, results, status, err = fut.result()
            except Exception as e:
                logger.exception("Scraper %s failed (executor): %s", module_name, e)
                continue

            if status == "timeout":
                logger.warning("Scraper %s timed out after %ss | engine=%s | url=%s", mod, timeout_s, engine, url)
                continue

            if status == "error":
                logger.error("Scraper %s failed | engine=%s:\n%s", mod, engine, err)
                continue

            total_found += found
            logger.info("Scraper %s: found=%s | fetch=%.2fs | engine=%s", mod, found, fetch_s, engine)

            if results is None:
                results = []
            if not isinstance(results, list):
                logger.error("Scraper %s returned non-list: %s", mod, type(results))
                continue

            filtered = list(results)

            if SWEDEN_ONLY:
                before = _safe_len(filtered)
                filtered = [a for a in filtered if _is_sweden_only(a)]
                logger.info(
                    "Scraper %s: kept_after_sweden=%s (dropped=%s)",
                    mod,
                    _safe_len(filtered),
                    before - _safe_len(filtered),
                )

            if wanted:
                before = _safe_len(filtered)
                filtered = [a for a in filtered if _matches_location_filter(a, wanted)]
                logger.info(
                    "Scraper %s: kept_after_location=%s (dropped=%s)",
                    mod,
                    _safe_len(filtered),
                    before - _safe_len(filtered),
                )

            reasons = Counter()
            validated = []
            for a in filtered:
                ok, reason = validate_assignment(a)
                if not ok:
                    reasons[reason] += 1
                    continue
                validated.append(a)

            dropped_q = sum(reasons.values())
            if dropped_q:
                top = ", ".join([f"{k}={v}" for k, v in reasons.most_common(6)])
                logger.info("Scraper %s: dropped_by_quality=%s (%s)", mod, dropped_q, top)
            else:
                logger.info("Scraper %s: dropped_by_quality=0", mod)

            kept = _safe_len(validated)
            total_kept += kept

            logger.info("Scraper %s: kept=%s after filters", mod, kept)
            save_assignments(validated)

    if OUTPUTS.get("excel", True):
        export_all()

    logger.info("Done. Total found=%s kept=%s", total_found, total_kept)


if __name__ == "__main__":
    mp.freeze_support()
    run()