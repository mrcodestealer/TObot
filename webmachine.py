"""
TObot copy of machine bot's ``webmachine.py`` — ONLY the background scrape loop
(no Flask dashboard): every WEBMACHINE_SCRAPE_INTERVAL_SEC (default 900s, min 60s) it
calls ``smmachine.smachine_collect_machines_all_deployments`` (warm browser per
backend/environment, walks EVERY machine in EVERY environment), normalizes + sorts the
rows, and persists them to ``webmachine_data.json`` (or WEBMACHINE_DATA_PATH).

Wire-up: call ``start_background_scrape_loop()`` once at startup (daemon thread;
no-op when WEBMACHINE_SCRAPE=0). Optionally call
``smmachine.prewarm_webmachine_scrape_pool_on_startup()`` right after.
Source regions (machine bot webmachine.py lines): 2577-2596, 2616-2667, 2674,
2682-2733, 2808-2863 — kept verbatim except one divergence: _background_worker
also catches SystemExit (a bad WEBMACHINE_SITES alias raises it in smmachine and
would otherwise kill the scrape thread silently and permanently).
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass


_scrape_lock = threading.Lock()
_scrape_rows: list[dict] = []
_scrape_errs: dict[str, str] = {}
_scrape_ts: float = 0.0
_bg_started = False
_bg_lock = threading.Lock()


def _truthy_env(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _scrape_enabled() -> bool:
    """Live EGM scrape is **on** unless ``WEBMACHINE_SCRAPE`` is explicitly ``0`` / ``false`` / ``no`` / ``off``."""
    return (os.environ.get("WEBMACHINE_SCRAPE") or "").strip().lower() not in ("0", "false", "no", "off")


def _data_json_path() -> Path:
    custom = (os.environ.get("WEBMACHINE_DATA_PATH") or "").strip()
    return Path(custom) if custom else (_ROOT / "webmachine_data.json")

def _persist_scrape_to_data_file(rows: list[dict]) -> None:
    """Write latest scrape snapshot for reload / backup; skipped when ``WEBMACHINE_JSON`` inline is used."""
    if (os.environ.get("WEBMACHINE_JSON") or "").strip():
        return
    p = _data_json_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "environment": r.get("environment"),
                "belongs": r.get("belongs"),
                "name": r.get("name"),
                "game_type": r.get("game_type"),
                "status": r.get("status"),
                "online": r.get("online_raw") or r.get("online_label"),
                "is_test": bool(r.get("is_test")),
            }
            for r in rows
        ]
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def _cell(row: dict, *keys: str) -> str:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def _online_pill(raw: str) -> tuple[str, str]:
    s = " ".join((raw or "").lower().split())
    if "offline" in s:
        return "Offline", "pill-offline"
    if "online" in s:
        return "Online", "pill-online"
    t = (raw or "").strip() or "—"
    return t, "pill-unknown"


def _infer_is_test(raw: dict, name: str) -> bool:
    v = raw.get("is_test")
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if str(v or "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    n = (name or "").lower()
    return "(test)" in n

_KNOWN_DEPLOYMENTS = frozenset({"PROD", "QAT", "UAT"})

def _normalize_rows(raw: object) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        inner = raw.get("machines") or raw.get("rows") or raw.get("data")
        if isinstance(inner, list):
            raw = inner
        else:
            return []
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        env = _cell(row, "environment", "env", "site", "backend")
        deployment = _cell(row, "deployment", "tier", "env_tier").upper()
        belongs = _cell(row, "belongs", "venue", "site_belong", "site_code")
        if deployment not in _KNOWN_DEPLOYMENTS:
            if env.upper() in _KNOWN_DEPLOYMENTS:
                deployment = env.upper()
            else:
                deployment = "PROD"
        if not belongs:
            if env and env.upper() not in _KNOWN_DEPLOYMENTS:
                belongs = env
            else:
                belongs = _cell(row, "site", "backend") or "—"
        name = _cell(row, "name", "machine", "machine_name", "id")
        game_type = _cell(row, "game_type", "game", "game_name", "gameType")
        status = _cell(row, "status", "machine_status", "state_detail")
        online_raw = _cell(row, "online", "online_offline", "conn", "reachability")
        if not online_raw:
            st = _cell(row, "state")
            if st and st.lower() in ("online", "offline"):
                online_raw = st
        label, pill = _online_pill(online_raw)
        is_test = _infer_is_test(row, name)
        out.append(
            {
                "environment": deployment,
                "belongs": belongs or "—",
                "name": name or "—",
                "game_type": game_type or "—",
                "status": status or "—",
                "online_label": label,
                "online_raw": online_raw or "—",
                "pill_class": pill,
                "is_test": is_test,
            }
        )
    return out

def _run_scrape_once() -> None:
    global _scrape_rows, _scrape_errs, _scrape_ts
    try:
        from smmachine import smachine_collect_machines_all_deployments
    except Exception as e:
        with _scrape_lock:
            _scrape_errs = {"_import": repr(e)}
            _scrape_ts = time.time()
            _scrape_rows = []
        return
    try:
        raw_rows, errs = smachine_collect_machines_all_deployments()
    except Exception as e:
        raw_rows, errs = [], {"_fatal": repr(e)}
    norm = _normalize_rows(raw_rows)
    norm.sort(
        key=lambda r: (
            (r.get("environment") or "").lower(),
            (r.get("belongs") or "").lower(),
            (r.get("name") or "").lower(),
        )
    )
    with _scrape_lock:
        _scrape_rows = norm
        _scrape_errs = errs
        _scrape_ts = time.time()
    _persist_scrape_to_data_file(norm)


def _background_worker() -> None:
    while True:
        if _scrape_enabled():
            try:
                _run_scrape_once()
            # Divergence from upstream (except Exception): smmachine raises SystemExit
            # for a bad WEBMACHINE_SITES alias, which would silently kill this thread.
            except (Exception, SystemExit) as e:
                with _scrape_lock:
                    _scrape_errs["_worker"] = repr(e)
                    _scrape_ts = time.time()
        try:
            interval = int((os.environ.get("WEBMACHINE_SCRAPE_INTERVAL_SEC") or "900").strip() or "900")
        except ValueError:
            interval = 900
        time.sleep(max(60, interval))


def start_background_scrape_loop() -> None:
    """Start daemon thread that refreshes scrape cache (runs when scrape is not explicitly disabled)."""
    global _bg_started
    if not _scrape_enabled():
        return
    with _bg_lock:
        if _bg_started:
            return
        _bg_started = True
    th = threading.Thread(target=_background_worker, name="webmachine-scrape", daemon=True)
    th.start()
