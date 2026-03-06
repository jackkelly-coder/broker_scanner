import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


@dataclass(frozen=True)
class BrowserConfig:
    headless: bool = True
    default_timeout_ms: int = 8000
    navigation_timeout_ms: int = 15000
    user_agent: Optional[str] = None
    locale: Optional[str] = None

    # Performance toggles
    block_heavy_resources: bool = True  # images/fonts/media
    java_script_enabled: bool = True  # sometimes can be False for static sites
    extra_http_headers: Optional[dict] = None

    # Chromium launch tuning
    launch_args: Optional[list[str]] = None


def _default_launch_args() -> list[str]:
    # Conservative, broadly safe speed/stability flags for headless scraping.
    return [
        "--disable-dev-shm-usage",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-extensions",
        "--disable-sync",
        "--no-first-run",
        "--no-default-browser-check",
        "--mute-audio",
        # Not always needed, but can help reduce weirdness
        "--disable-features=Translate,BackForwardCache",
    ]


def _install_resource_blocker(context) -> None:
    """
    Abort heavy resources for big speed wins. Keep scripts + XHR for CSR.
    """
    def handler(route):
        try:
            rtype = route.request.resource_type
            if rtype in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()
        except Exception:
            try:
                route.continue_()
            except Exception:
                pass

    try:
        context.route("**/*", handler)
    except Exception:
        # If routing fails for any reason, just continue without blocking.
        pass


@contextmanager
def browser_context(conf: BrowserConfig = BrowserConfig()):
    """
    Usage:
        with browser_context() as ctx:
            page = ctx.new_page()
            ...
    """
    launch_args = conf.launch_args or _default_launch_args()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=conf.headless, args=launch_args)

        context_args = {
            "java_script_enabled": conf.java_script_enabled,
        }
        if conf.user_agent:
            context_args["user_agent"] = conf.user_agent
        if conf.locale:
            context_args["locale"] = conf.locale

        context = browser.new_context(**context_args)

        if conf.extra_http_headers:
            try:
                context.set_extra_http_headers(conf.extra_http_headers)
            except Exception:
                pass

        context.set_default_timeout(conf.default_timeout_ms)
        context.set_default_navigation_timeout(conf.navigation_timeout_ms)

        if conf.block_heavy_resources:
            _install_resource_blocker(context)

        try:
            yield context
        finally:
            try:
                context.close()
            finally:
                browser.close()


def goto(
    page,
    url: str,
    wait_until: str = "domcontentloaded",
    timeout_ms: Optional[int] = None,
    retries: int = 0,
    retry_sleep_ms: int = 250,
) -> bool:
    """
    Safe navigation. Returns True if navigation succeeded, else False.

    - timeout_ms: override per-call timeout (else context default)
    - retries: retry count on failure (useful for flaky CSR/CDN)
    """
    last_err = None
    for attempt in range(retries + 1):
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            return True
        except Exception as e:
            last_err = e
            if attempt < retries:
                try:
                    page.wait_for_timeout(retry_sleep_ms)
                except Exception:
                    pass
                continue
            return False
    return False


def wait_for_any_selector(page, selectors: Iterable[str], timeout_ms: int = 8000) -> Optional[str]:
    """
    Wait for the first selector that appears, within a TOTAL timeout budget.
    Returns the selector that matched, else None.

    Important: timeout_ms is total, not per-selector.
    """
    sels = [s for s in selectors if s]
    if not sels:
        return None

    start = time.time()
    remaining = timeout_ms

    # Allocate time fairly across selectors, but keep it adaptive.
    for i, sel in enumerate(sels):
        elapsed_ms = int((time.time() - start) * 1000)
        remaining = timeout_ms - elapsed_ms
        if remaining <= 0:
            return None

        # Give each remaining selector at most its "share", but never less than 250ms.
        share = max(250, remaining // max(1, (len(sels) - i)))

        try:
            page.wait_for_selector(sel, timeout=share)
            return sel
        except PlaywrightTimeoutError:
            continue
        except Exception:
            continue

    return None


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_text(
    locator,
    timeout_ms: int = 1200,
    normalize_ws: bool = True,
    state: str = "attached",
    prefer_text_content: bool = True,
) -> str:
    """
    Reads text without throwing if element never appears.

    Performance notes:
    - text_content() is usually faster and triggers less layout work than inner_text().
    - inner_text() can be more "what user sees" but can be slower on heavy pages.

    Params:
    - state: "attached" (fast) or "visible" (more user-like, slower)
    - prefer_text_content: True => text_content(), False => inner_text()
    """
    try:
        first = locator.first
        first.wait_for(state=state, timeout=timeout_ms)

        if prefer_text_content:
            txt = (first.text_content() or "").strip()
        else:
            txt = (first.inner_text() or "").strip()

        return _normalize_ws(txt) if normalize_ws else txt
    except Exception:
        return ""


def safe_attr(locator, name: str, timeout_ms: int = 1200, state: str = "attached") -> str:
    try:
        first = locator.first
        first.wait_for(state=state, timeout=timeout_ms)
        v = first.get_attribute(name)
        return (v or "").strip()
    except Exception:
        return ""


def dismiss_cookie_banners(page) -> None:
    """
    Best-effort: try clicking common cookie buttons.
    Never throws.
    """
    candidates = [
        # Swedish / English common
        'button:has-text("Neka")',
        'button:has-text("Neka alla")',
        'button:has-text("Avvisa")',
        'button:has-text("Reject")',
        'button:has-text("Reject all")',
        'button:has-text("Decline")',
        'button:has-text("Only necessary")',
        'button:has-text("Godkänn endast nödvändiga")',
        # Generic close
        '[aria-label="Close"]',
        '[aria-label="Stäng"]',
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel)
            if btn.count() > 0:
                btn.first.click(timeout=700)
                return
        except Exception:
            pass


def dedup_by_id(items: Iterable[dict]) -> list[dict]:
    uniq = {}
    for it in items:
        if not it:
            continue
        _id = it.get("id")
        if not _id:
            continue
        uniq[_id] = it
    return list(uniq.values())


def normalize_title(title: str) -> str:
    return _normalize_ws(title)


def normalize_location(location: str) -> str:
    return _normalize_ws(location)