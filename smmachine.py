# TObot copy of machine bot's smmachine.py — ONLY the read-only webmachine scrape:
# warm browser pool (one persistent browser per backend/environment), the all-deployment
# machine walk (PROD/QAT/UAT x all backend sites), and the prewarm-on-startup hook.
# CLI tick/report modes and the Lark prod-batch bot sections were NOT copied.
# Source regions (machine bot smmachine.py lines): 1-100, 222-257, 291-604, 673-848,
# 951-991, 1022-1046, 1298-1317, 1417-2113 — kept verbatim so diffs against upstream stay clean.
#!/usr/bin/env python3
"""
SM machine list — login routing matches ``checkcredit.py`` backends; tick table row checkboxes with pagination.

Usage::

    python3 smmachine.py nwr
    1932
    NCH1933
    nch1922
    <press Enter on an empty line to finish — or press Ctrl+D>

Or one line (no stdin needed)::

    python3 smmachine.py nwr 1932 NCH1933 nch1922

**Batch maintenance + test** (same as webapp ``set_both``; opens a **headed** browser by default)::

    python3 smmachine.py maintenancetest nch1422
    python3 smmachine.py maintenancetest "Dragons Trio-NCH1462"

Optional remark: ``SM_BATCH_REMARK=your note`` (max 100 chars on the EGM dialog).

**Batch toolbar dry-run** (maintenance/test buttons only; opens dialog then **Cancel** — never Save)::

    python3 smmachine.py batchbuttontest
    python3 smmachine.py batchbuttontest nch cp wf

Tests: ``BatchMaintenance``, ``BatchTest``, ``BatchStart Using``, ``BatchTestCancel`` only
(ignores ``BatchKick Out``, ``Sync DB Config``, …).

First argument is a **site alias** (which backend / login to open):

- ``nwr``, ``np`` → NP (``backend-np``), synthetic route ``NWR0001``
- ``nch``, ``nc``, ``new`` → NCH (``backend-nc``)
- **Check status (read-only):** alias suffix ``cs``. Groups found machines under headings like ``Machine in online, maintain, no test mode`` then lists names; **only non-empty groups** are printed. **Test** = ``span.test`` or ``(TEST)`` in text. **Not found** section only if any request is missing.
- ``tbr`` → TBR (``backend-tbr``)
- ``tbp``, ``mdr``, ``dhs``, ``cp``, ``osm``, ``wf``, ``winford`` → same mapping as ``checkcredit``

Credentials: same env / ``.env`` as Duty Bot (``NP_BACKEND_*``, ``NCH_BACKEND_*``, ``TBR_BACKEND_*``, …).

Flow:

1. Login and open the machine table (default ``/egm/egmStatusList``; override with ``SM_MACHINE_PATH``).
2. Ensure pagination is on **first** page (Previous until disabled).
3. In **request order**, find each machine; **only tick** if Status is **normal** or **occupy** and Online/Offline is **online**. Rows in **maintenance** / **offline** / other statuses are **not ticked**; if their checkbox is on, it is cleared. Those machines are listed before the backward pass.
4. If some targets remain unfound, click **Next**, repeat (bounded by ``NP_BACKEND_MAX_PAGES`` / ``SM_MACHINE_MAX_PAGES``).
5. After every **eligible** row is ticked, walk **backward** with **Previous** through every page visited; on each page re-verify checkboxes for ticked machines only (do not assume).
6. Print machine row labels that are still checked; then AFK ``SM_MACHINE_AFK_SEC`` (default **90**) seconds.

Env:

- ``SM_MACHINE_PATH`` — path after host (default ``/egm/egmStatusList``).
- ``SM_MACHINE_AFK_SEC`` — idle seconds at end (default ``90``).
- ``SM_MACHINE_MAX_PAGES`` — max Next steps for **CLI** tick/report (default: ``NP_BACKEND_MAX_PAGES``, often 20).
- ``SM_MACHINE_COLLECT_MAX_PAGES`` — for **read-only** ``smachine_collect_all_machine_rows`` / web dashboard only:
  max Next steps when ``SM_MACHINE_MAX_PAGES`` is **unset** (default **500** so full machine lists are not cut off early).
- ``SM_MACHINE_HEADLESS=1`` — headless Chromium (default: headed unless Linux without DISPLAY).
- ``SM_MACHINE_HEADED=1`` — force headed (used by ``maintenancetest`` mode).
- ``SM_BATCH_REMARK`` — optional remark for ``maintenancetest`` / batch EGM save dialog.
- ``SM_MACHINE_STRICT_BACKWARD=1`` — do not re-tick on backward verify if checkboxes were cleared by paging (Element UI tables often drop selection across pages unless ``reserve-selection`` is enabled).

Programmatic read-only export (for dashboards / ``webapp``):

- ``smachine_collect_all_machine_rows(site, …)`` — one backend, all table pages (read-only); returns ``(rows, truncation_warning)``.
- ``smachine_collect_machines_multi_sites()`` — all default backends (deduped by EGM URL); ``WEBMACHINE_SITES`` overrides.
- ``WEBMACHINE_WARM_POOL=1`` (default) — keep one **headed** browser open per backend for ``webmachine_data.json`` refresh; set ``WEBMACHINE_WARM_POOL=0`` for one-shot launch/close scrapes.
"""

from __future__ import annotations

import json
import logging
import os
import queue as _queue
import re
import sys
import tempfile
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Optional, Sequence
from urllib.parse import quote

logger = logging.getLogger(__name__)

_ROOT_DIR = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT_DIR / ".env")
except ImportError:
    pass


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _site_routing_key(site: str) -> str:
    """
    ``<alias>cs`` (suffix ``cs`` = check status) routes like ``<alias>`` for backend / credentials.
    Example: ``mdrcs`` → ``mdr``, ``nchcs`` → ``nch``.
    """
    s = (site or "").strip().lower()
    if len(s) > 2 and s.endswith("cs"):
        return s[:-2]
    return s


def _site_synthetic_machine(site: str) -> str:
    """Machine label that routes ``checkcredit._np_resolve_backend`` to the desired host."""
    s = _site_routing_key(site)
    aliases: dict[str, str] = {
        "nwr": "NWR0001",
        "np": "NWR0001",
        "nch": "NCH0001",
        "nc": "NCH0001",
        "new": "NCH0001",
        "tbr": "TBR0001",
        "tbp": "TBP0001",
        "mdr": "MDR0001",
        "dhs": "DHS0001",
        "cp": "CP0001",
        "osm": "OSM0001",
        "wf": "WF0001",
        "winford": "WF0001",
    }
    syn = aliases.get(s)
    if not syn:
        raise SystemExit(
            f"Unknown site alias {site!r}. Try: {', '.join(sorted(set(aliases.keys())))}"
        )
    return syn

def _site_belongs_label(site_key: str) -> str:
    """Venue / property code for dashboard ``belongs`` column (PROD site aliases)."""
    labels = {
        "nwr": "NP",
        "np": "NP",
        "nch": "NCH",
        "nc": "NCH",
        "new": "NCH",
        "tbr": "TBR",
        "tbp": "TBP",
        "mdr": "MDR",
        "dhs": "DHS",
        "cp": "CP",
        "osm": "CP",
        "wf": "WF",
        "winford": "WF",
    }
    return labels.get((site_key or "").strip().lower(), (site_key or "").upper())


def _osmslot_admin_credentials() -> tuple[str, str]:
    user = (os.environ.get("WEBMACHINE_OSMSLOT_USER") or os.environ.get("OSMSLOT_ADMIN_USER") or "admin").strip()
    pw = (os.environ.get("WEBMACHINE_OSMSLOT_PASSWORD") or os.environ.get("OSMSLOT_ADMIN_PASSWORD") or "123456").strip()
    return user, pw


def _nonprod_backend_specs(deployment: str) -> list[dict[str, str | bool]]:
    """QAT / UAT EGM backends on ``*.osmslot.org`` (see webapp deployment tabs)."""
    dep = (deployment or "").strip().upper()
    if dep not in ("QAT", "UAT"):
        return []
    prefix = "qat" if dep == "QAT" else "uat"
    user, pw = _osmslot_admin_credentials()
    hosts: tuple[tuple[str, str], ...] = (
        ("CP", f"https://{prefix}-cp.osmslot.org"),
        ("TBP", f"https://{prefix}-tbp.osmslot.org"),
        ("TBR", f"https://{prefix}-tbr.osmslot.org"),
        ("DHS", f"https://{prefix}-dhs.osmslot.org"),
        ("NCH", f"https://{prefix}-nc.osmslot.org"),
        ("WF", f"https://{prefix}-wf.osmslot.org"),
        ("MDR", f"https://{prefix}-mdr.osmslot.org"),
        ("NP", f"https://{prefix}-np.osmslot.org"),
    )
    out: list[dict[str, str | bool]] = []
    for belongs, base in hosts:
        out.append(
            {
                "belongs": belongs,
                "base": base,
                "user": user,
                "password": pw,
                "deployment": dep,
                "dismiss_warning_dialog": dep == "QAT",
                "list_path": "/egm/egmStatusList",
                "login_path": "/login",
            }
        )
    return out


def _dismiss_warning_dialog(page, timeout_ms: int) -> None:
    """Close Element UI ``Warnning`` modal (QAT) via header X before reading the EGM table."""
    try:
        dialog = page.locator('.el-dialog[aria-label="Warnning"], .el-dialog:has(.el-dialog__title:has-text("Warnning"))').first
        if dialog.count() == 0:
            return
        close = dialog.locator(".el-dialog__headerbtn[aria-label='Close'], .el-dialog__headerbtn").first
        if close.count() and close.is_visible(timeout=min(5000, timeout_ms)):
            close.click()
            page.wait_for_timeout(450)
    except Exception:
        pass


def _resolve_collect_page_limit(max_pages: int | None) -> int:
    from checkcredit import NP_BACKEND_MAX_PAGES  # noqa: WPS433

    if max_pages is None:
        explicit = (os.environ.get("SM_MACHINE_MAX_PAGES") or "").strip()
        if explicit:
            try:
                return max(1, int(explicit))
            except ValueError:
                return max(1, NP_BACKEND_MAX_PAGES)
        try:
            collect_cap = int((os.environ.get("SM_MACHINE_COLLECT_MAX_PAGES") or "500").strip() or "500")
        except ValueError:
            collect_cap = 500
        return max(1, collect_cap)
    return max(1, int(max_pages))


def _smachine_egm_urls(
    base_url: str,
    list_path: str = "/egm/egmStatusList",
    login_path: str = "/login",
) -> tuple[str, str, str]:
    base = (base_url or "").strip().rstrip("/")
    path = (list_path or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    if not path.startswith("/"):
        path = "/" + path
    login = (login_path or "/login").strip() or "/login"
    if not login.startswith("/"):
        login = "/" + login
    login_url = f"{base}{login}?redirect={quote(path, safe='')}"
    list_url = f"{base}{path}"
    return base, login_url, list_url


def _smachine_login_and_open_egm_list(
    page,
    *,
    base_url: str,
    username: str,
    password: str,
    list_path: str = "/egm/egmStatusList",
    login_path: str = "/login",
    dismiss_warning_dialog: bool = False,
    timeout_ms: int = 120_000,
    stall_check: Callable[[], bool] | None = None,
) -> str:
    """Log in and navigate to the EGM status table; returns ``list_url``."""
    _base, login_url, list_url = _smachine_egm_urls(base_url, list_path, login_path)
    path = list_url.split(_base, 1)[-1] if _base else "/egm/egmStatusList"

    def _maybe_stall(where: str) -> None:
        if stall_check and stall_check():
            raise RuntimeError(f"EGM scrape stalled ({where}; no progress detected)")

    page.goto(login_url, wait_until="domcontentloaded")
    page.wait_for_timeout(900)
    _maybe_stall("login page")

    pwd_box = page.locator('input[type="password"]').first
    pwd_box.wait_for(state="visible", timeout=min(30_000, timeout_ms))
    _maybe_stall("login form")
    form = pwd_box.locator("xpath=ancestor::form[1]")
    user = (username or "").strip()
    pw = (password or "").strip()
    if form.count():
        tin = form.locator(
            'input[type="text"], input:not([type]), input[type="tel"], input[type="email"]'
        ).first
        tin.fill(user)
    else:
        page.locator('input[type="text"]').first.fill(user)
    pwd_box.fill(pw)
    lb = page.get_by_role("button", name=re.compile(r"login|sign in|log in", re.I))
    if lb.count():
        lb.first.click()
    else:
        page.locator('button[type="submit"], button.el-button--primary').first.click()

    page.wait_for_timeout(1800)
    _maybe_stall("after login")
    if dismiss_warning_dialog:
        _dismiss_warning_dialog(page, timeout_ms)
    if path not in (page.url or ""):
        page.goto(list_url, wait_until="domcontentloaded")
    if dismiss_warning_dialog:
        _dismiss_warning_dialog(page, timeout_ms)

    page.wait_for_selector(".app-container, .filter-container, .el-table", timeout=timeout_ms)
    _wait_table_idle(page, timeout_ms)
    _maybe_stall("machine table")
    return list_url


def _smachine_collect_rows_on_egm_page(
    page,
    *,
    belongs: str,
    deployment: str,
    max_pages: int | None = None,
    timeout_ms: int = 120_000,
    stall_check: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[dict], str | None]:
    """Walk the EGM table on an already-logged-in page (read-only)."""
    limit = _resolve_collect_page_limit(max_pages)
    dep_label = (deployment or "PROD").strip().upper() or "PROD"
    belong_label = (belongs or "—").strip() or "—"
    collected: list[dict] = []
    trunc_msg: str | None = None

    def _tick(pages: int, rows: int) -> None:
        if on_progress:
            on_progress(pages, rows)

    def _maybe_stall(where: str) -> None:
        if stall_check and stall_check():
            raise RuntimeError(f"EGM scrape stalled ({where}; no progress detected)")

    _tick(0, 0)
    _go_first_page(page, timeout_ms=timeout_ms, max_steps=limit)
    _wait_table_idle(page, timeout_ms)
    expected_total = _pagination_total_entries(page)

    next_clicks = 0
    while True:
        _maybe_stall("pagination")
        for mn, test, game_type, st, onl in _collect_visible_table_machine_rows(page, timeout_ms=timeout_ms):
            collected.append(
                {
                    "environment": dep_label,
                    "belongs": belong_label,
                    "name": mn,
                    "game_type": game_type,
                    "status": st,
                    "online": onl,
                    "is_test": test,
                }
            )

        _tick(next_clicks + 1, len(collected))

        if not _can_pagination_next(page):
            break
        if next_clicks >= limit:
            try:
                if _can_pagination_next(page):
                    trunc_msg = (
                        f"pagination stopped after {limit} page(s); more data exists — "
                        "raise SM_MACHINE_COLLECT_MAX_PAGES or set SM_MACHINE_MAX_PAGES"
                    )
            except Exception:
                trunc_msg = f"pagination stopped after {limit} page(s) (could not verify Next)"
            break
        _click_pagination_next(page, timeout_ms=timeout_ms)
        next_clicks += 1
        _wait_table_idle(page, timeout_ms)
    if expected_total is not None and len(collected) < expected_total:
        note = (
            f"table reports {expected_total} entries but collected {len(collected)} "
            f"for {belong_label} @ {dep_label}"
        )
        trunc_msg = f"{trunc_msg}; {note}" if trunc_msg else note
    return collected, trunc_msg


def smachine_collect_rows_at_backend(
    *,
    base_url: str,
    username: str,
    password: str,
    belongs: str,
    deployment: str,
    list_path: str = "/egm/egmStatusList",
    login_path: str = "/login",
    dismiss_warning_dialog: bool = False,
    headless: bool | None = None,
    max_pages: int | None = None,
    timeout_ms: int = 120_000,
    stall_check: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[dict], str | None]:
    """
    Log in to one explicit EGM origin, optionally dismiss the QAT warning dialog, walk
    ``/egm/egmStatusList`` (read-only), and return normalized rows for webapp.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise RuntimeError("Install playwright: pip install playwright && playwright install chromium") from e

    base = (base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("empty base_url")
    user = (username or "").strip()
    pw = (password or "").strip()
    if not user or not pw:
        raise RuntimeError(f"missing credentials for {belongs!r} @ {deployment}")

    hl = _smachine_resolve_headless(headless)

    def _maybe_stall(where: str) -> None:
        if stall_check and stall_check():
            raise RuntimeError(f"EGM scrape stalled ({where}; no progress detected)")

    if on_progress:
        on_progress(0, 0)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=hl)
        try:
            context = browser.new_context(
                viewport={"width": 1600, "height": 900},
                ignore_https_errors=True,
            )
            page = context.new_page()
            page.set_default_timeout(timeout_ms)

            _smachine_login_and_open_egm_list(
                page,
                base_url=base,
                username=user,
                password=pw,
                list_path=list_path,
                login_path=login_path,
                dismiss_warning_dialog=dismiss_warning_dialog,
                timeout_ms=timeout_ms,
                stall_check=stall_check,
            )
            return _smachine_collect_rows_on_egm_page(
                page,
                belongs=belongs,
                deployment=deployment,
                max_pages=max_pages,
                timeout_ms=timeout_ms,
                stall_check=stall_check,
                on_progress=on_progress,
            )
        finally:
            browser.close()

def _wait_table_idle(page, timeout_ms: int) -> None:
    try:
        page.wait_for_function(
            "() => !Array.from(document.querySelectorAll('.el-loading-mask')).some(x => x && x.offsetParent !== null)",
            timeout=min(timeout_ms, 30_000),
        )
    except Exception:
        pass
    page.wait_for_timeout(350)


def _pagination_root(page):
    """Prefer the list page footer inside ``.app-container`` (dialogs often teleport outside)."""
    scoped = page.locator(".app-container .el-pagination")
    if scoped.count():
        return scoped.first
    return page.locator(".el-pagination").first


def _pagination_prev_btn(page):
    return _pagination_root(page).locator("button.btn-prev").first


def _pagination_next_btn(page):
    return _pagination_root(page).locator("button.btn-next").first


def _can_pagination_prev(page) -> bool:
    btn = _pagination_prev_btn(page)
    if btn.count() == 0:
        return False
    try:
        return not btn.is_disabled()
    except Exception:
        return False


def _can_pagination_next(page) -> bool:
    btn = _pagination_next_btn(page)
    if btn.count() == 0:
        return False
    try:
        return not btn.is_disabled()
    except Exception:
        return False


def _blocking_modal_present(page) -> bool:
    """
    True when a visible Element UI modal overlay (message-box / dialog) could intercept clicks.

    Note: these wrappers are ``position: fixed``, so ``offsetParent`` is ``null`` even when visible —
    visibility must be judged from computed style + bounding rect, not ``offsetParent``.
    """
    try:
        return bool(
            page.evaluate(
                """() => {
                  for (const s of ['.el-message-box__wrapper', '.el-dialog__wrapper']) {
                    for (const el of document.querySelectorAll(s)) {
                      const st = getComputedStyle(el);
                      if (st.display === 'none' || st.visibility === 'hidden'
                          || parseFloat(st.opacity || '1') < 0.05) continue;
                      const r = el.getBoundingClientRect();
                      if (r.width > 0 && r.height > 0) return true;
                    }
                  }
                  return false;
                }"""
            )
        )
    except Exception:
        return False


def _dismiss_intercepting_modal(page, *, timeout_ms: int) -> bool:
    """
    Clear a leftover modal overlay (Element UI message-box / dialog) that is intercepting clicks.

    Safe for the mutation flow: it NEVER clicks a two-button confirm's primary/apply button. It only
    presses Escape, clicks Cancel/Close/No (or the header X), or — for a single-button notice
    (``$alert``) — clicks its lone acknowledge button. So a stale popup is cleared without applying
    any unintended backend action. Returns ``True`` if no blocking modal remains.
    """
    if not _blocking_modal_present(page):
        return False

    # 1) Escape — cancels (never confirms) a $confirm; closes $alert where allowed.
    for _ in range(2):
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass
        page.wait_for_timeout(200)
        if not _blocking_modal_present(page):
            return True

    # 2) Cancel / Close / No, or the header X — on the topmost message-box or dialog.
    for wrap_sel in (".el-message-box__wrapper", ".el-dialog__wrapper"):
        layer = page.locator(wrap_sel).last
        if layer.count() == 0:
            continue
        try:
            if not layer.is_visible():
                continue
        except Exception:
            continue
        for name_pat in (r"^cancel$", r"^close$", r"^no$", r"取消", r"关闭"):
            btn = layer.get_by_role("button", name=re.compile(name_pat, re.I))
            if btn.count():
                try:
                    btn.first.click(timeout=min(10_000, timeout_ms))
                    page.wait_for_timeout(250)
                    if not _blocking_modal_present(page):
                        return True
                except Exception:
                    continue
        x = layer.locator(".el-dialog__headerbtn, .el-message-box__headerbtn").first
        if x.count():
            try:
                x.click(timeout=min(10_000, timeout_ms))
                page.wait_for_timeout(250)
                if not _blocking_modal_present(page):
                    return True
            except Exception:
                pass

    # 3) Single-button notice ($alert): its only button is an acknowledge — safe to click.
    mb = page.locator(".el-message-box__wrapper").filter(has=page.locator(".el-message-box")).last
    if mb.count():
        try:
            btns = mb.locator(".el-message-box__btns button")
            if btns.count() == 1:
                btns.first.click(timeout=min(10_000, timeout_ms))
                page.wait_for_timeout(250)
        except Exception:
            pass

    return not _blocking_modal_present(page)


def _click_pagination_prev(page, *, timeout_ms: int) -> None:
    btn = _pagination_prev_btn(page)
    btn.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    if _blocking_modal_present(page):
        _dismiss_intercepting_modal(page, timeout_ms=timeout_ms)
    try:
        btn.click(timeout=min(30_000, timeout_ms))
    except Exception:
        if _dismiss_intercepting_modal(page, timeout_ms=timeout_ms):
            btn.click(timeout=min(30_000, timeout_ms))
        else:
            raise
    page.wait_for_timeout(900)


def _click_pagination_next(page, *, timeout_ms: int) -> None:
    btn = _pagination_next_btn(page)
    btn.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    if _blocking_modal_present(page):
        _dismiss_intercepting_modal(page, timeout_ms=timeout_ms)
    try:
        btn.click(timeout=min(30_000, timeout_ms))
    except Exception:
        if _dismiss_intercepting_modal(page, timeout_ms=timeout_ms):
            btn.click(timeout=min(30_000, timeout_ms))
        else:
            raise
    page.wait_for_timeout(900)


def _go_first_page(page, *, timeout_ms: int, max_steps: int) -> None:
    for _ in range(max_steps + 5):
        if not _can_pagination_prev(page):
            return
        _click_pagination_prev(page, timeout_ms=timeout_ms)

def _cell_raw_text(cell, *, timeout_ms: int) -> str:
    """
    Prefer ``text_content()`` for full subtree text; use with ``span.test`` detection because ``(TEST)``
    may be CSS-only (not in text nodes).
    """
    t = min(8_000, timeout_ms)
    try:
        tc = cell.text_content(timeout=t)
        if tc is not None and tc.strip():
            return tc
    except Exception:
        pass
    try:
        return cell.inner_text(timeout=t) or ""
    except Exception:
        return ""


def _cell_text_one_line(cell, *, timeout_ms: int) -> str:
    raw = _cell_raw_text(cell, timeout_ms=timeout_ms)
    return " ".join((raw or "").strip().split())


def _machine_name_cell_test_mode_and_display(cell, *, timeout_ms: int) -> tuple[bool, str]:
    """
    Detect EGM test row: Vue uses ``<div>…name…</div><span class="test"></span>``; ``(TEST)`` is often
    **not** in the DOM (only ``::after`` / CSS), so ``textContent`` misses it. Fallback: literal ``(TEST)`` in text.
    """
    name_line = _cell_text_one_line(cell, timeout_ms=timeout_ms)
    literal = bool(re.search(r"\(TEST\)", name_line or "", re.I))
    span_test = False
    try:
        span_test = cell.locator("span.test").first.count() > 0
    except Exception:
        span_test = False
    is_test = literal or span_test
    if is_test and span_test and not literal:
        display = f"{name_line}(TEST)" if name_line else "(TEST)"
    else:
        display = name_line
    return is_test, display

def _row_report_fields(row, *, timeout_ms: int) -> tuple[str, bool, str, str, str]:
    """
    Machine name (col 1), test mode, Game Type (col 2), Status (col 7),
    Online/Offline (col 8). Returns ``(machine_name, is_test_mode, game_type, status_text, online_or_offline)``.
    """
    cells = row.locator("td.el-table__cell")
    try:
        n = cells.count()
    except Exception:
        n = 0
    if n >= 2:
        is_test, name = _machine_name_cell_test_mode_and_display(cells.nth(1), timeout_ms=timeout_ms)
    else:
        is_test, name = False, ""
    game_type = _cell_text_one_line(cells.nth(2), timeout_ms=timeout_ms) if n >= 3 else ""
    status = _cell_text_one_line(cells.nth(6), timeout_ms=timeout_ms) if n >= 7 else ""
    online_raw = _cell_text_one_line(cells.nth(7), timeout_ms=timeout_ms) if n >= 8 else ""
    ol = " ".join((online_raw or "").lower().split())
    if "offline" in ol:
        online_disp = "offline"
    elif "online" in ol:
        online_disp = "online"
    else:
        online_disp = online_raw or "(unknown)"
    return name, is_test, game_type, status, online_disp

def _table_body_rows(page):
    """
    Data rows only (not header). Target **main** scroll body only — fixed-column tables also use
    ``tr.el-table__row`` and duplicate rows; loose selectors pick clones whose checkbox does not
    reflect the real selection.
    """
    strict = page.locator(
        "div.el-table__body-wrapper > table.el-table__body > tbody > tr.el-table__row"
    )
    if strict.count():
        return strict
    primary = page.locator(
        ".el-table__body-wrapper:not(.el-table__fixed-body-wrapper) tbody tr.el-table__row"
    )
    if primary.count():
        return primary
    fallback = page.locator(".el-table__body tbody tr.el-table__row")
    if fallback.count():
        return fallback
    return page.locator(".el-table__body tr.el-table__row")

def _pagination_total_entries(page) -> int | None:
    """Parse Element UI footer text like ``Showing 1 to 200 of 247 entries``."""
    try:
        txt = _pagination_root(page).inner_text(timeout=5_000) or ""
    except Exception:
        return None
    m = re.search(r"of\s+([\d,]+)\s+entries", txt, re.I)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _collect_visible_table_machine_rows(page, *, timeout_ms: int) -> list[tuple[str, bool, str, str, str]]:
    """All data rows on the current page: ``(machine_name, is_test, game_type, status, online_word)``."""
    rows = _table_body_rows(page)
    try:
        rows.first.wait_for(state="visible", timeout=min(15_000, timeout_ms))
    except Exception:
        pass
    out: list[tuple[str, bool, str, str, str]] = []
    try:
        n = rows.count()
    except Exception:
        n = 0
    for i in range(n):
        row = rows.nth(i)
        try:
            mn, test, game_type, st, onl = _row_report_fields(row, timeout_ms=timeout_ms)
        except Exception:
            continue
        name = (mn or "").strip()
        if not name:
            continue
        out.append((name, test, (game_type or "").strip(), (st or "").strip(), (onl or "").strip()))
    return out


def _smachine_resolve_headless(headless: bool | None) -> bool:
    if headless is not None:
        return bool(headless)
    if _truthy_env("BOT_PLAYWRIGHT_HEADLESS") or _truthy_env("PLAYWRIGHT_HEADLESS"):
        return True
    if _truthy_env("SM_MACHINE_HEADLESS"):
        return True
    if _truthy_env("SM_MACHINE_HEADED"):
        return False
    return sys.platform == "linux" and not (os.environ.get("DISPLAY") or "").strip()


def smachine_collect_all_machine_rows(
    site: str,
    *,
    headless: bool | None = None,
    max_pages: int | None = None,
    timeout_ms: int = 120_000,
    stall_check: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[dict], str | None]:
    """
    Log in to one backend (same routing as CLI), walk the EGM status table from first page forward,
    and return every visible row (**read-only**, no checkbox changes, no other UI actions).

    Pagination: if ``max_pages`` is ``None`` and ``SM_MACHINE_MAX_PAGES`` is unset, uses
    ``SM_MACHINE_COLLECT_MAX_PAGES`` (default **500**) so large sites are fully walked. Set
    ``SM_MACHINE_MAX_PAGES`` to override with the same knob as the CLI.

    Returns ``(rows, truncation_warning)`` where ``truncation_warning`` is set if the table still
    had a enabled **Next** when the page cap was hit (list may be incomplete).
    """
    from checkcredit import _np_resolve_backend  # noqa: WPS433

    site_key = _site_routing_key(site or "")
    if not site_key:
        raise ValueError("empty site")
    try:
        synth = _site_synthetic_machine(site)
    except SystemExit as e:
        raise ValueError(str(e)) from e
    base, user, pw = _np_resolve_backend(synth)
    if not user or not pw:
        raise RuntimeError(f"missing backend credentials for {site_key!r}")

    path = (os.environ.get("SM_MACHINE_PATH") or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    return smachine_collect_rows_at_backend(
        base_url=base,
        username=user,
        password=pw,
        belongs=_site_belongs_label(site_key),
        deployment="PROD",
        list_path=path,
        login_path="/login",
        dismiss_warning_dialog=False,
        headless=headless,
        max_pages=max_pages,
        timeout_ms=timeout_ms,
        stall_check=stall_check,
        on_progress=on_progress,
    )


def _dedupe_site_keys_by_resolved_backend(site_keys: list[str]) -> tuple[list[str], dict[str, str]]:
    """
    Each distinct EGM origin (``base_url`` + login user) is scraped once; later aliases that map to
    the same login (e.g. ``osm`` after ``cp`` on ``backend.osmplay.com``) are skipped with a note.
    """
    from checkcredit import _np_resolve_backend  # noqa: WPS433

    seen: dict[tuple[str, str], str] = {}
    order: list[str] = []
    skipped: dict[str, str] = {}
    for sk in site_keys:
        try:
            synth = _site_synthetic_machine(sk)
        except SystemExit:
            order.append(sk)
            continue
        try:
            base, u, pw = _np_resolve_backend(synth)
        except Exception:
            order.append(sk)
            continue
        if not pw:
            order.append(sk)
            continue
        key = (base.rstrip("/"), (u or "").strip())
        if key in seen:
            skipped[sk] = f"skipped — same EGM as {seen[key]!r}"
            continue
        seen[key] = sk
        order.append(sk)
    return order, skipped


DEFAULT_WEBMACHINE_SITES: tuple[str, ...] = ("nwr", "nch", "tbr", "tbp", "mdr", "dhs", "cp", "osm", "wf")


def _scrape_concurrency(item_count: int) -> int:
    """
    Max concurrent EGM page scrapes (each runs its own headless Chromium).

    Controlled by ``WEBMACHINE_SCRAPE_CONCURRENCY`` (default **8**):
    * ``0`` (or negative) → **unlimited** = open *all* pages at the same time.
    * ``1`` → old sequential behaviour.
    Capped to the number of items so we never start idle workers.
    """
    try:
        n = int((os.environ.get("WEBMACHINE_SCRAPE_CONCURRENCY") or "8").strip() or "8")
    except ValueError:
        n = 8
    if n <= 0:  # unlimited → one worker per page (all at once)
        return max(1, item_count)
    return max(1, min(n, max(1, item_count)))


# A scrape "unit": (label, callable) where the callable returns ``(rows, warning)``.
ScrapeUnit = tuple[str, Callable[[], tuple[list[dict], Optional[str]]]]


def _collect_units(units: list[ScrapeUnit]) -> tuple[list[dict], dict[str, str]]:
    """
    Run every scrape unit in parallel (up to :func:`_scrape_concurrency`), so all EGM pages refresh
    at once instead of one-by-one. ``units`` may span sites *and* deployments — the whole set shares
    one thread pool, which is what lets PROD/QAT/UAT load simultaneously.
    """
    errs: dict[str, str] = {}
    all_rows: list[dict] = []
    workers = _scrape_concurrency(len(units))

    if workers <= 1 or len(units) <= 1:
        for label, fn in units:
            try:
                part, twarn = fn()
                all_rows.extend(part)
                if twarn:
                    errs[label] = twarn
            except Exception as e:  # noqa: BLE001
                errs[label] = str(e)
        return all_rows, errs

    results: dict[str, tuple[tuple[list[dict], str | None] | None, Exception | None]] = {}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sm-scrape") as ex:
        future_map = {ex.submit(fn): label for label, fn in units}
        for fut in as_completed(future_map):
            label = future_map[fut]
            try:
                results[label] = (fut.result(), None)
            except Exception as e:  # noqa: BLE001
                results[label] = (None, e)
    for label, _fn in units:
        res, err = results.get(label, (None, None))
        if err is not None:
            errs[label] = str(err)
            continue
        if res is None:
            continue
        part, twarn = res
        all_rows.extend(part)
        if twarn:
            errs[label] = twarn
    return all_rows, errs


def _collect_concurrently(
    keys: list[str],
    worker: Callable[[str], tuple[list[dict], str | None]],
) -> tuple[list[dict], dict[str, str]]:
    """Backwards-compatible wrapper: run ``worker(key)`` for every key as scrape units."""
    units: list[ScrapeUnit] = [(k, (lambda k=k: worker(k))) for k in keys]
    return _collect_units(units)


def _prod_scrape_units(sites: Sequence[str] | None = None, **kwargs: Any) -> tuple[list[ScrapeUnit], dict[str, str]]:
    """PROD scrape units (one per deduped backend site) + skipped-alias notes."""
    raw_env = (os.environ.get("WEBMACHINE_SITES") or "").strip()
    if sites is not None:
        use = [s.strip().lower() for s in sites if (s or "").strip()]
    elif raw_env:
        use = [s.strip().lower() for s in raw_env.split(",") if s.strip()]
    else:
        use = list(DEFAULT_WEBMACHINE_SITES)
    use, skipped = _dedupe_site_keys_by_resolved_backend(use)
    units: list[ScrapeUnit] = [
        (sk, (lambda sk=sk: smachine_collect_all_machine_rows(sk, **kwargs))) for sk in use
    ]
    return units, dict(skipped)


def _nonprod_scrape_units(deployment: str, **kwargs: Any) -> tuple[list[ScrapeUnit], dict[str, str]]:
    """QAT/UAT scrape units (one per ``*.osmslot.org`` backend)."""
    dep = (deployment or "").strip().upper()
    specs = _nonprod_backend_specs(dep)
    if not specs:
        return [], {dep: f"unsupported deployment {deployment!r}"}
    units: list[ScrapeUnit] = []
    for spec in specs:
        key = f"{dep}:{spec['belongs']}"
        units.append(
            (
                key,
                (
                    lambda spec=spec: smachine_collect_rows_at_backend(
                        base_url=str(spec["base"]),
                        username=str(spec["user"]),
                        password=str(spec["password"]),
                        belongs=str(spec["belongs"]),
                        deployment=dep,
                        list_path=str(spec["list_path"]),
                        login_path=str(spec["login_path"]),
                        dismiss_warning_dialog=bool(spec["dismiss_warning_dialog"]),
                        **kwargs,
                    )
                ),
            )
        )
    return units, {}


def smachine_collect_machines_multi_sites(
    sites: Sequence[str] | None = None,
    **kwargs: Any,
) -> tuple[list[dict], dict[str, str]]:
    """
    Scrape several site aliases **concurrently** (thread pool, see ``WEBMACHINE_SCRAPE_CONCURRENCY``).
    ``kwargs`` are passed to ``smachine_collect_all_machine_rows`` (e.g. ``headless=``,
    ``max_pages=``, ``timeout_ms=``).

    Returns ``(rows, errors_by_site_key)`` where ``errors_by_site_key`` holds per-site failure or
    truncation messages (and skipped-alias notes from :func:`_dedupe_site_keys_by_resolved_backend`).

    Default site list: ``DEFAULT_WEBMACHINE_SITES`` (every routed backend from ``checkcredit``) or
    env ``WEBMACHINE_SITES`` (comma-separated).
    """
    units, skipped = _prod_scrape_units(sites, **kwargs)
    rows, errs = _collect_units(units)
    # Keep skipped-alias notes alongside scrape errors.
    merged = dict(skipped)
    merged.update(errs)
    return rows, merged


def smachine_collect_nonprod_deployment(
    deployment: str,
    **kwargs: Any,
) -> tuple[list[dict], dict[str, str]]:
    """Scrape every QAT or UAT ``*.osmslot.org`` backend in :func:`_nonprod_backend_specs`."""
    units, errs = _nonprod_scrape_units(deployment, **kwargs)
    if not units:
        return [], errs
    rows, scrape_errs = _collect_units(units)
    errs.update(scrape_errs)
    return rows, errs


# ---------------------------------------------------------------------------
# Webmachine warm browser pool (keep EGM browsers open for webmachine_data.json)
# ---------------------------------------------------------------------------
# One persistent, headed Chromium per backend (PROD site + QAT/UAT host). Browsers stay open
# between scrapes; the background webapp thread re-walks tables and writes webmachine_data.json.
# Disable with ``WEBMACHINE_WARM_POOL=0``. Headed by default when warm pool is on
# (``WEBMACHINE_WARM_HEADLESS=1`` to hide windows).

BackendScrapeSpec = dict[str, Any]
_WEBMACHINE_WARM_KEEPALIVE_SEC = 240.0


def _webmachine_warm_pool_enabled() -> bool:
    return (os.environ.get("WEBMACHINE_WARM_POOL", "1") or "").strip().lower() not in (
        "0", "false", "no", "off",
    )


def _webmachine_warm_prewarm_on_startup() -> bool:
    return (os.environ.get("WEBMACHINE_WARM_PREWARM_ON_STARTUP", "1") or "").strip().lower() not in (
        "0", "false", "no", "off",
    )


def _webmachine_warm_headless() -> bool:
    if _truthy_env("WEBMACHINE_WARM_HEADLESS"):
        return True
    if _truthy_env("WEBMACHINE_WARM_HEADED") or _truthy_env("SM_MACHINE_HEADED"):
        return False
    if _webmachine_warm_pool_enabled():
        return False
    return _smachine_resolve_headless(None)


def _wm_warm_profile_dir(label: str) -> Path:
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", (label or "backend").strip())[:48]
    return Path(tempfile.gettempdir()) / f"wm_warm_profile_{safe}"


def _prod_backend_spec_for_site(site: str) -> BackendScrapeSpec:
    from checkcredit import _np_resolve_backend  # noqa: WPS433

    site_key = _site_routing_key(site or "")
    synth = _site_synthetic_machine(site)
    base, user, pw = _np_resolve_backend(synth)
    path = (os.environ.get("SM_MACHINE_PATH") or "/egm/egmStatusList").strip() or "/egm/egmStatusList"
    return {
        "base_url": base,
        "username": user,
        "password": pw,
        "belongs": _site_belongs_label(site_key),
        "deployment": "PROD",
        "list_path": path,
        "login_path": "/login",
        "dismiss_warning_dialog": False,
    }


def _nonprod_backend_spec(spec: dict[str, Any]) -> BackendScrapeSpec:
    return {
        "base_url": str(spec["base"]),
        "username": str(spec["user"]),
        "password": str(spec["password"]),
        "belongs": str(spec["belongs"]),
        "deployment": str(spec["deployment"]),
        "list_path": str(spec["list_path"]),
        "login_path": str(spec["login_path"]),
        "dismiss_warning_dialog": bool(spec["dismiss_warning_dialog"]),
    }


def _all_deployment_backend_specs(**kwargs: Any) -> tuple[list[tuple[str, BackendScrapeSpec]], dict[str, str]]:
    """Backend login specs for every configured deployment (same scope as full scrape)."""
    raw = (os.environ.get("WEBMACHINE_DEPLOYMENTS") or "prod,qat,uat").strip()
    deployments = [d.strip().upper() for d in raw.split(",") if d.strip()]
    if not deployments:
        deployments = ["PROD"]

    specs: list[tuple[str, BackendScrapeSpec]] = []
    errs: dict[str, str] = {}
    for dep in deployments:
        if dep == "PROD":
            raw_env = (os.environ.get("WEBMACHINE_SITES") or "").strip()
            if raw_env:
                use = [s.strip().lower() for s in raw_env.split(",") if s.strip()]
            else:
                use = list(DEFAULT_WEBMACHINE_SITES)
            use, skipped = _dedupe_site_keys_by_resolved_backend(use)
            errs.update(skipped)
            for sk in use:
                try:
                    specs.append((sk, _prod_backend_spec_for_site(sk)))
                except Exception as e:  # noqa: BLE001
                    errs[sk] = str(e)
        elif dep in ("QAT", "UAT"):
            for spec in _nonprod_backend_specs(dep):
                key = f"{dep}:{spec['belongs']}"
                specs.append((key, _nonprod_backend_spec(spec)))
        else:
            errs[dep] = f"unknown deployment {dep!r}"
    return specs, errs


class _WebmachineScrapeWarm:
    """One long-lived EGM browser for a single backend (read-only scrape for webmachine_data.json)."""

    def __init__(self, label: str, spec: BackendScrapeSpec) -> None:
        self.label = label
        self.spec = dict(spec)
        self._tasks: _queue.Queue[dict] = _queue.Queue()
        self._p = None
        self._context = None
        self._page = None
        self._list_url = ""
        self._thread = threading.Thread(
            target=self._loop, name=f"wm-warm-{label}", daemon=True
        )
        self._thread.start()

    def submit_prewarm(self) -> None:
        self._tasks.put({"kind": "prewarm"})

    def submit_keepalive(self) -> None:
        self._tasks.put({"kind": "keepalive"})

    def collect(self, **kwargs: Any) -> tuple[list[dict], str | None]:
        done = threading.Event()
        box: dict[str, Any] = {}
        self._tasks.put({"kind": "collect", "kwargs": kwargs, "done": done, "box": box})
        done.wait()
        if box.get("error"):
            raise RuntimeError(str(box["error"]))
        return list(box.get("rows") or []), box.get("warn")

    def _loop(self) -> None:
        while True:
            task = self._tasks.get()
            kind = task.get("kind")
            if kind == "prewarm":
                try:
                    self._ensure_ready(task.get("timeout_ms") or 120_000)
                    print(f"[wm-warm:{self.label}] pre-warmed (browser stays open).", flush=True)
                except Exception as ex:
                    print(f"[wm-warm:{self.label}] prewarm failed: {ex!r}", flush=True)
                    self._teardown()
                continue
            if kind == "keepalive":
                try:
                    if self._healthy():
                        self._refresh_table(task.get("timeout_ms") or 120_000)
                except Exception:
                    self._teardown()
                continue
            if kind == "collect":
                box = task["box"]
                try:
                    timeout_ms = int(task["kwargs"].get("timeout_ms") or 120_000)
                    self._ensure_ready(timeout_ms)
                    self._refresh_table(timeout_ms)
                    rows, warn = _smachine_collect_rows_on_egm_page(
                        self._page,
                        belongs=str(self.spec.get("belongs") or "—"),
                        deployment=str(self.spec.get("deployment") or "PROD"),
                        max_pages=task["kwargs"].get("max_pages"),
                        timeout_ms=timeout_ms,
                        stall_check=task["kwargs"].get("stall_check"),
                        on_progress=task["kwargs"].get("on_progress"),
                    )
                    box["rows"] = rows
                    box["warn"] = warn
                except Exception as ex:
                    box["error"] = ex
                    self._teardown()
                finally:
                    task["done"].set()

    def _healthy(self) -> bool:
        try:
            return self._page is not None and not self._page.is_closed()
        except Exception:
            return False

    def _launch(self) -> None:
        from playwright.sync_api import sync_playwright

        self._teardown()
        self._p = sync_playwright().start()
        profile = _wm_warm_profile_dir(self.label)
        profile.mkdir(parents=True, exist_ok=True)
        self._context = self._p.chromium.launch_persistent_context(
            user_data_dir=str(profile),
            headless=_webmachine_warm_headless(),
            viewport={"width": 1600, "height": 900},
            ignore_https_errors=True,
        )
        self._page = (
            self._context.pages[0] if self._context.pages else self._context.new_page()
        )
        print(f"[wm-warm:{self.label}] browser launched (kept open).", flush=True)

    def _teardown(self) -> None:
        for closer in (
            lambda: self._context.close() if self._context else None,
            lambda: self._p.stop() if self._p else None,
        ):
            try:
                closer()
            except Exception:
                pass
        self._context = None
        self._page = None
        self._p = None
        self._list_url = ""

    def _ensure_ready(self, timeout_ms: int) -> None:
        if not self._healthy():
            self._launch()
            self._page.set_default_timeout(timeout_ms)
            self._list_url = _smachine_login_and_open_egm_list(
                self._page,
                base_url=str(self.spec["base_url"]),
                username=str(self.spec["username"]),
                password=str(self.spec["password"]),
                list_path=str(self.spec.get("list_path") or "/egm/egmStatusList"),
                login_path=str(self.spec.get("login_path") or "/login"),
                dismiss_warning_dialog=bool(self.spec.get("dismiss_warning_dialog")),
                timeout_ms=timeout_ms,
            )

    def _refresh_table(self, timeout_ms: int) -> None:
        if not self._healthy():
            return
        try:
            if self._list_url:
                self._page.goto(self._list_url, wait_until="domcontentloaded", timeout=timeout_ms)
            if self.spec.get("dismiss_warning_dialog"):
                _dismiss_warning_dialog(self._page, timeout_ms)
            limit = _resolve_collect_page_limit(None)
            _go_first_page(self._page, timeout_ms=timeout_ms, max_steps=limit)
            _wait_table_idle(self._page, timeout_ms)
        except Exception:
            pass


class _WebmachineWarmPool:
    def __init__(self) -> None:
        self._workers: dict[str, _WebmachineScrapeWarm] = {}
        self._lock = threading.Lock()
        self._keepalive = threading.Thread(
            target=self._keepalive_loop, name="wm-warm-keepalive", daemon=True
        )
        self._keepalive.start()

    def _get(self, label: str, spec: BackendScrapeSpec) -> _WebmachineScrapeWarm:
        with self._lock:
            w = self._workers.get(label)
            if w is None:
                w = _WebmachineScrapeWarm(label, spec)
                self._workers[label] = w
            return w

    def prewarm_specs(self, specs: list[tuple[str, BackendScrapeSpec]]) -> None:
        for label, spec in specs:
            self._get(label, spec).submit_prewarm()

    def collect_specs(
        self, specs: list[tuple[str, BackendScrapeSpec]], **kwargs: Any
    ) -> tuple[list[dict], dict[str, str]]:
        errs: dict[str, str] = {}
        all_rows: list[dict] = []
        workers_n = _scrape_concurrency(len(specs))

        def _one(label: str, spec: BackendScrapeSpec) -> tuple[str, list[dict], str | None, Exception | None]:
            try:
                rows, warn = self._get(label, spec).collect(**kwargs)
                return label, rows, warn, None
            except Exception as e:  # noqa: BLE001
                return label, [], None, e

        if workers_n <= 1 or len(specs) <= 1:
            for label, spec in specs:
                _label, rows, warn, err = _one(label, spec)
                if err is not None:
                    errs[label] = str(err)
                    continue
                all_rows.extend(rows)
                if warn:
                    errs[label] = warn
            return all_rows, errs

        results: dict[str, tuple[list[dict], str | None, Exception | None]] = {}
        with ThreadPoolExecutor(max_workers=workers_n, thread_name_prefix="wm-warm-collect") as ex:
            futs = {ex.submit(_one, label, spec): label for label, spec in specs}
            for fut in as_completed(futs):
                label, rows, warn, err = fut.result()
                results[label] = (rows, warn, err)
        for label, _spec in specs:
            rows, warn, err = results.get(label, ([], None, None))
            if err is not None:
                errs[label] = str(err)
                continue
            all_rows.extend(rows)
            if warn:
                errs[label] = warn
        return all_rows, errs

    def _keepalive_loop(self) -> None:
        while True:
            time.sleep(_WEBMACHINE_WARM_KEEPALIVE_SEC)
            with self._lock:
                workers = list(self._workers.values())
            for w in workers:
                w.submit_keepalive()


_webmachine_warm_pool_singleton: _WebmachineWarmPool | None = None
_webmachine_warm_pool_lock = threading.Lock()


def _webmachine_warm_pool() -> _WebmachineWarmPool:
    global _webmachine_warm_pool_singleton
    with _webmachine_warm_pool_lock:
        if _webmachine_warm_pool_singleton is None:
            _webmachine_warm_pool_singleton = _WebmachineWarmPool()
        return _webmachine_warm_pool_singleton


def prewarm_webmachine_scrape_pool_on_startup() -> None:
    """Launch + EGM-login one persistent browser per backend for webmachine_data.json (stays open)."""
    if not _webmachine_warm_pool_enabled():
        print("[wm-warm] disabled (WEBMACHINE_WARM_POOL=0).", flush=True)
        return
    if not _webmachine_warm_prewarm_on_startup():
        print("[wm-warm] startup pre-warm skipped (WEBMACHINE_WARM_PREWARM_ON_STARTUP=0).", flush=True)
        return
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
    except ImportError:
        print(
            "[wm-warm] startup pre-warm skipped — playwright not installed "
            "(pip install playwright && playwright install chromium). "
            "Set WEBMACHINE_WARM_POOL=0 to silence.",
            flush=True,
        )
        return
    specs, skipped = _all_deployment_backend_specs()
    if skipped:
        for k, v in skipped.items():
            print(f"[wm-warm] note {k}: {v}", flush=True)
    if not specs:
        print("[wm-warm] no backends configured — nothing to pre-warm.", flush=True)
        return
    labels = ", ".join(lbl for lbl, _ in specs)
    print(f"[wm-warm] startup pre-warm ({len(specs)} browser(s), kept open): {labels}", flush=True)
    try:
        _webmachine_warm_pool().prewarm_specs(specs)
    except Exception as ex:
        print(f"[wm-warm] startup pre-warm failed: {ex!r}", flush=True)


def smachine_collect_machines_all_deployments(
    **kwargs: Any,
) -> tuple[list[dict], dict[str, str]]:
    """
    Scrape configured deployments (``WEBMACHINE_DEPLOYMENTS``, default ``prod,qat,uat``).

    All backends across **all** deployments are loaded in a **single shared thread pool**, so
    PROD/QAT/UAT pages open at the same time (subject to ``WEBMACHINE_SCRAPE_CONCURRENCY``; set it
    to ``0`` for truly unlimited / everything at once). This minimises the staleness window.

    When ``WEBMACHINE_WARM_POOL=1`` (default), each backend uses a **persistent headed browser**
    that stays open between scrapes (for ``webmachine_data.json`` background refresh).
    """
    if _webmachine_warm_pool_enabled():
        specs, errs = _all_deployment_backend_specs(**kwargs)
        rows, scrape_errs = _webmachine_warm_pool().collect_specs(specs, **kwargs)
        errs.update(scrape_errs)
        return rows, errs

    raw = (os.environ.get("WEBMACHINE_DEPLOYMENTS") or "prod,qat,uat").strip()
    deployments = [d.strip().upper() for d in raw.split(",") if d.strip()]
    if not deployments:
        deployments = ["PROD"]

    units: list[ScrapeUnit] = []
    errs: dict[str, str] = {}
    for dep in deployments:
        if dep == "PROD":
            dep_units, skipped = _prod_scrape_units(**kwargs)
            units.extend(dep_units)
            errs.update(skipped)
        elif dep in ("QAT", "UAT"):
            dep_units, dep_err = _nonprod_scrape_units(dep, **kwargs)
            units.extend(dep_units)
            errs.update(dep_err)
        else:
            errs[dep] = f"unknown deployment {dep!r}"

    rows, scrape_errs = _collect_units(units)
    errs.update(scrape_errs)
    return rows, errs
