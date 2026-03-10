import importlib
import logging
import multiprocessing as mp
import threading
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import config
from app_config import OUTPUTS, SCRAPERS, ScraperConfig
from database import init_db, save_assignments, sync_assignments, log_scraper_result
from export import export_all
from geo import is_sweden_assignment
from quality import validate_assignment
from utils import canonicalize_url

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


@dataclass
class ScraperResult:
    module: str
    fetch_s: float
    found: int
    results: list[dict]
    status: str
    error: str = ""


def _safe_len(value: Any) -> int:
    try:
        return len(value)
    except Exception:
        return 0


def _get_location_filter() -> str:
    return (config.LOCATION_FILTER or "").strip().lower()


def _matches_location_filter(item: dict, wanted: str) -> bool:
    location = (item.get("location") or "").lower()
    title = (item.get("title") or "").lower()
    return wanted in location or wanted in title


def _is_sweden_only(item: dict) -> bool:
    return is_sweden_assignment(item.get("location") or "", item.get("title") or "")


def _child_logging_init() -> None:
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _scraper_process_entry(conn, module_name: str, url: str) -> None:
    _child_logging_init()
    try:
        started = time.perf_counter()
        module = importlib.import_module(f"scrapers.{module_name}")
        results = module.fetch(url)
        duration = time.perf_counter() - started

        if results is None:
            results = []
        if not isinstance(results, list):
            raise TypeError(f"{module_name}.fetch() must return list[dict], got {type(results)}")

        conn.send(("ok", module_name, duration, len(results), results))
    except Exception:
        conn.send(("err", module_name, traceback.format_exc()))
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _run_scraper_with_timeout(module_name: str, url: str, timeout_s: int) -> ScraperResult:
    parent_conn, child_conn = config.MP_CTX.Pipe(duplex=False)
    process = config.MP_CTX.Process(
        target=_scraper_process_entry,
        args=(child_conn, module_name, url),
        daemon=True,
    )

    try:
        process.start()
    except Exception as exc:
        try:
            parent_conn.close()
            child_conn.close()
        except Exception:
            pass
        return ScraperResult(module_name, 0.0, 0, [], "error", f"Failed to start subprocess: {exc}")

    try:
        child_conn.close()
    except Exception:
        pass

    process.join(timeout_s)

    if process.is_alive():
        process.terminate()
        process.join(5)
        try:
            parent_conn.close()
        except Exception:
            pass
        return ScraperResult(module_name, 0.0, 0, [], "timeout", f"Timed out after {timeout_s}s")

    try:
        if parent_conn.poll(1.0):
            message = parent_conn.recv()
        else:
            return ScraperResult(
                module_name,
                0.0,
                0,
                [],
                "error",
                "Scraper process exited without sending a result",
            )
    except Exception as exc:
        return ScraperResult(module_name, 0.0, 0, [], "error", f"Failed reading result: {exc}")
    finally:
        try:
            parent_conn.close()
        except Exception:
            pass

    if message[0] == "ok":
        _, mod, duration, found, results = message
        return ScraperResult(mod, float(duration), int(found), results, "ok")

    _, mod, tb = message
    return ScraperResult(mod, 0.0, 0, [], "error", tb)


def _run_scraper_direct(module_name: str, url: str) -> ScraperResult:
    try:
        started = time.perf_counter()
        module = importlib.import_module(f"scrapers.{module_name}")
        results = module.fetch(url)
        duration = time.perf_counter() - started

        if results is None:
            results = []
        if not isinstance(results, list):
            raise TypeError(f"{module_name}.fetch() must return list[dict], got {type(results)}")

        return ScraperResult(module_name, float(duration), len(results), results, "ok")
    except Exception:
        return ScraperResult(module_name, 0.0, 0, [], "error", traceback.format_exc())


def _run_scraper_once(module_name: str, url: str, timeout_s: int) -> ScraperResult:
    if config.USE_SUBPROCESS:
        return _run_scraper_with_timeout(module_name, url, timeout_s)
    return _run_scraper_direct(module_name, url)


def _run_scraper_with_retry(module_name: str, url: str, timeout_s: int) -> ScraperResult:
    retries = int(getattr(config, "SCRAPER_RETRIES", 1))
    backoff_s = float(getattr(config, "SCRAPER_RETRY_BACKOFF_S", 1.5))

    attempts = max(1, retries + 1)
    last_result = ScraperResult(module_name, 0.0, 0, [], "error", "No attempts executed")

    for attempt in range(1, attempts + 1):
        result = _run_scraper_once(module_name, url, timeout_s)

        if result.status == "ok":
            if attempt > 1:
                logger.info("Scraper %s recovered on retry %s/%s", module_name, attempt, attempts)
            return result

        last_result = result
        logger.warning(
            "Scraper %s failed attempt %s/%s | status=%s",
            module_name,
            attempt,
            attempts,
            result.status,
        )

        if attempt < attempts:
            time.sleep(backoff_s * attempt)

    return last_result


def _filter_and_validate(results: list[dict], wanted_location: str, module_name: str) -> tuple[list[dict], dict]:
    filtered = list(results or [])
    stats: dict[str, Any] = {"quality_reasons": {}}

    if config.SWEDEN_ONLY:
        before = _safe_len(filtered)
        filtered = [item for item in filtered if _is_sweden_only(item)]
        logger.info(
            "Scraper %s: kept_after_sweden=%s (dropped=%s)",
            module_name,
            _safe_len(filtered),
            before - _safe_len(filtered),
        )

    if wanted_location:
        before = _safe_len(filtered)
        filtered = [item for item in filtered if _matches_location_filter(item, wanted_location)]
        logger.info(
            "Scraper %s: kept_after_location=%s (dropped=%s)",
            module_name,
            _safe_len(filtered),
            before - _safe_len(filtered),
        )

    reasons = Counter()
    validated: list[dict] = []
    for item in filtered:
        ok, reason = validate_assignment(item)
        if not ok:
            reasons[reason] += 1
            continue
        validated.append(item)

    stats["quality_reasons"] = dict(reasons)

    if reasons:
        top = ", ".join(f"{key}={value}" for key, value in reasons.most_common(6))
        logger.info("Scraper %s: dropped_by_quality=%s (%s)", module_name, sum(reasons.values()), top)
    else:
        logger.info("Scraper %s: dropped_by_quality=0", module_name)

    logger.info("Scraper %s: kept=%s after filters", module_name, len(validated))
    return validated, stats


def run() -> None:
    wanted = _get_location_filter()
    http_sem = threading.Semaphore(max(1, config.HTTP_WORKERS))
    browser_sem = threading.Semaphore(max(1, config.BROWSER_WORKERS))

    logger.info(
        "Starting run. Sweden-only=%s | Location filter='%s' | Scrapers=%s | "
        "MAX_WORKERS=%s HTTP_WORKERS=%s BROWSER_WORKERS=%s USE_SUBPROCESS=%s (mp=spawn)",
        config.SWEDEN_ONLY,
        wanted if wanted else "<none>",
        len(SCRAPERS),
        config.MAX_WORKERS,
        config.HTTP_WORKERS,
        config.BROWSER_WORKERS,
        config.USE_SUBPROCESS,
    )

    init_db()

    total_found = 0
    total_kept = 0
    total_inserted = 0
    total_updated = 0
    total_skipped_in_save = 0
    all_current_urls: set[str] = set()

    def run_gated(scraper: ScraperConfig) -> tuple[ScraperConfig, ScraperResult]:
        sem = browser_sem if scraper.engine == "browser" else http_sem
        logger.info("Waiting slot: %s | engine=%s", scraper.module, scraper.engine)
        with sem:
            logger.info(
                "Running: %s | engine=%s | timeout=%ss",
                scraper.module,
                scraper.engine,
                scraper.timeout_s,
            )
            result = _run_scraper_with_retry(scraper.module, scraper.url, scraper.timeout_s)
            return scraper, result

    futures = {}
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        for scraper in SCRAPERS:
            logger.info("Queued: %s | engine=%s", scraper.module, scraper.engine)
            future = executor.submit(run_gated, scraper)
            futures[future] = scraper

        for future in as_completed(futures):
            scraper = futures[future]

            try:
                _, result = future.result()
                timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                log_scraper_result(scraper.module, result.status, timestamp, result.error)
            except Exception as exc:
                logger.exception("Scraper %s crashed in executor: %s", scraper.module, exc)
                continue

            if result.status == "timeout":
                logger.warning(
                    "Scraper %s timed out after %ss | engine=%s | url=%s",
                    scraper.module,
                    scraper.timeout_s,
                    scraper.engine,
                    scraper.url,
                )
                continue

            if result.status == "error":
                logger.error(
                    "Scraper %s failed | engine=%s:\n%s",
                    scraper.module,
                    scraper.engine,
                    result.error,
                )
                continue

            total_found += result.found
            logger.info(
                "Scraper %s: found=%s | fetch=%.2fs | engine=%s",
                scraper.module,
                result.found,
                result.fetch_s,
                scraper.engine,
            )

            validated, stats = _filter_and_validate(result.results, wanted, scraper.module)
            logger.info(
                "Scraper %s: raw=%s validated=%s stats=%s",
                scraper.module,
                len(result.results),
                len(validated),
                stats,
            )
            total_kept += len(validated)

            save_stats = save_assignments(validated)
            logger.info("Scraper %s: save_stats=%s", scraper.module, save_stats)

            for item in validated:
                if item.get("url"):
                    all_current_urls.add(canonicalize_url(item["url"]))


            total_inserted += save_stats["inserted"]
            total_updated += save_stats["updated"]
            total_skipped_in_save += save_stats["skipped"]

            # Sync: remove old assignments not in current scrape
            # Sync: remove old assignments not in current scrape


            logger.info(
                "Scraper %s: batch_duplicates=%s db_inserted=%s db_updated=%s skipped_in_save=%s",
                scraper.module,
                save_stats["batch_duplicates"],
                save_stats["inserted"],
                save_stats["updated"],
                save_stats["skipped"],
            )


        total_deleted = 0

        if all_current_urls:
            total_deleted = sync_assignments(all_current_urls)
            logger.info("Final sync deleted=%s", total_deleted)
        else:
            logger.warning("Final sync skipped because all_current_urls was empty")
        
        if OUTPUTS.get("excel", True):
            export_stats = export_all()
            logger.info("Exported rows=%s", export_stats["rows"])

    logger.info(
        "Done. Total found=%s kept=%s inserted=%s updated=%s deleted=%s skipped_in_save=%s",
        total_found,
        total_kept,
        total_inserted,
        total_updated,
        total_deleted,
        total_skipped_in_save,
    )


if __name__ == "__main__":
    mp.freeze_support()
    run()