"""
TObot copy of machine bot's ``checkcredit.py`` — ONLY the PROD backend resolution
used by ``smmachine.py`` (``_np_resolve_backend`` + ``NP_BACKEND_MAX_PAGES``): maps a
machine label (NWR/NCH/DHS/CP/OSM/MDR/TBR/TBP/WF) to (base_url, username, password)
via *_BACKEND_USER / *_BACKEND_PASSWORD env vars. Same module name as upstream so
``from checkcredit import _np_resolve_backend`` inside smmachine.py works unchanged.
Source regions (machine bot checkcredit.py lines): 2834-2841, 2852-2870, 2882-2991, 3015-3065.
"""
from __future__ import annotations

import os
import re

# ----- NP backend — Log Third Http Req (Duty Bot /npthirdhttp) -----
NP_BACKEND_DEFAULT_BASE = os.environ.get("NP_BACKEND_BASE", "https://backend-np.osmplay.com").rstrip("/")
# ±minutes around parsed log time (time_short + date) for Log Third Http Search; override via env.
NP_BACKEND_WINDOW_MINUTES = int(os.environ.get("NP_BACKEND_WINDOW_MINUTES", "60"))
try:
    NP_BACKEND_MAX_PAGES = max(1, int(os.environ.get("NP_BACKEND_MAX_PAGES", "20").strip() or "20"))
except ValueError:
    NP_BACKEND_MAX_PAGES = 20

def _np_backend_env_cred() -> tuple[str, str]:
    u = (os.environ.get("NP_BACKEND_USER") or "").strip()
    p = (os.environ.get("NP_BACKEND_PASSWORD") or "").strip()
    return u, p


_WINFORD_NP_BASE = "https://backend-winford.osmplay.com".rstrip("/")
_DHS_BACKEND_BASE = "https://backend-dhs.osmplay.com".rstrip("/")
_NCH_BACKEND_BASE = "https://backend-nc.osmplay.com".rstrip("/")
_TBP_BACKEND_BASE = "https://backend-tbp.osmplay.com".rstrip("/")
_TBR_BACKEND_BASE = "https://backend-tbr.osmplay.com".rstrip("/")
_CP_BACKEND_BASE = "https://backend.osmplay.com".rstrip("/")
_MDR_BACKEND_BASE = "https://backend-midori.osmplay.com".rstrip("/")
# Public repo: no hardcoded fallback logins — set MDR_BACKEND_USER/PASSWORD and
# TBR_BACKEND_USER/PASSWORD in .env (osedutybot kept literals here).
_MDR_BACKEND_DEFAULT_USER = ""
_MDR_BACKEND_DEFAULT_PASSWORD = ""
_TBR_BACKEND_DEFAULT_USER = ""
_TBR_BACKEND_DEFAULT_PASSWORD = ""

def _np_use_dhs_log_backend(machine_display: str | None) -> bool:
    """DHS cabinet — folder / last path segment starts with ``DHS`` (e.g. ``DHS3178``, ``DHS8173``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)DHS", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("DHS"))


def _np_use_nch_log_backend(machine_display: str | None) -> bool:
    """NCH cabinet — folder / last path segment starts with ``NCH`` (e.g. ``NCH1171``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)NCH", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("NCH"))


def _np_use_cp_log_backend(machine_display: str | None) -> bool:
    """CP cabinet — folder / last path segment starts with ``CP`` (e.g. ``CP7178``, ``CP0231``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)CP", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("CP"))


def _np_use_osm_log_backend(machine_display: str | None) -> bool:
    """OSM cabinet — folder / last path segment starts with ``OSM`` (e.g. ``OSM7178``). Same backend as ``CP*``."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)OSM", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("OSM"))


def _np_use_backend_osmplay_com(machine_display: str | None) -> bool:
    """``https://backend.osmplay.com`` — ``CP*`` or ``OSM*`` (shared ``CP_BACKEND_*`` creds and EGM login redirect)."""
    return _np_use_cp_log_backend(machine_display) or _np_use_osm_log_backend(machine_display)


def _np_use_mdr_log_backend(machine_display: str | None) -> bool:
    """MDR cabinet — folder / last path segment starts with ``MDR`` (e.g. ``MDR7178``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)MDR", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("MDR"))


def _np_use_tbr_log_backend(machine_display: str | None) -> bool:
    """TBR cabinet — folder / last path segment starts with ``TBR`` (e.g. ``TBR1234``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)TBR", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("TBR"))


def _np_use_tbp_log_backend(machine_display: str | None) -> bool:
    """TBP cabinet — folder / last path segment starts with ``TBP`` (e.g. ``TBP8641``)."""
    raw = (machine_display or "").strip()
    if not raw:
        return False
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)TBP", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    return bool(alnum.startswith("TBP"))


def _np_use_winford_log_backend(machine_display: str | None) -> bool:
    """
    Use Winford ``backend-winford`` Log Third Http instead of NP.

    - Any **folder / last path segment** starting with ``WF`` (case-insensitive), e.g. ``WF8123``,
      ``WF8173``, ``MINIPC/WF8123``.
    - ``winford`` anywhere in the machine string.
    - ``NWR8173`` (alnum or substring): digits-only query ``8173`` → default OSS template ``NWR{n}``.
    """
    raw = (machine_display or "").strip()
    if not raw:
        return False
    if re.search(r"(?i)winford", raw):
        return True
    seg = raw.replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if seg and re.match(r"(?i)WF", seg):
        return True
    alnum = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    if alnum and (alnum == "NWR8173" or "NWR8173" in alnum):
        return True
    return False

def _np_resolve_backend(machine_display: str | None) -> tuple[str, str, str]:
    """
    (base_url, username, password) for Log Third Http Req.

    **DHS** (machine label ``DHS*``) → ``backend-dhs.osmplay.com`` + ``DHS_BACKEND_*`` (else ``NP_BACKEND_*``).
    **NCH** (machine label ``NCH*``) → ``backend-nc.osmplay.com`` + ``NCH_BACKEND_*`` (else ``NP_BACKEND_*``).
    **CP** / **OSM** (machine labels ``CP*`` / ``OSM*``) → ``backend.osmplay.com`` + ``CP_BACKEND_USER`` / ``CP_BACKEND_PASSWORD``.
    **MDR** (machine label ``MDR*``) → ``backend-midori.osmplay.com`` + ``MDR_BACKEND_*``
    (default ``mdr-omduty`` / ``mdr-omduty`` when unset).
    **TBR** (machine label ``TBR*``) → ``backend-tbr.osmplay.com`` + ``TBR_BACKEND_*``
    (default ``tromduty`` / ``tromduty`` when unset).
    **TBP** (machine label ``TBP*``) → ``backend-tbp.osmplay.com`` + ``TBP_BACKEND_*``.
    **Winford** (``WF*``, ``winford``, ``NWR8173`` OSS alias) → ``backend-winford`` + ``WF_BACKEND_*``
    (default ``omduty1``).
    Otherwise → ``NP_BACKEND_BASE`` / ``NP_BACKEND_USER`` / ``NP_BACKEND_PASSWORD``.
    """
    if _np_use_dhs_log_backend(machine_display):
        nu, npw = _np_backend_env_cred()
        u = (os.environ.get("DHS_BACKEND_USER") or nu).strip()
        p = (os.environ.get("DHS_BACKEND_PASSWORD") or npw).strip()
        return _DHS_BACKEND_BASE, u, p
    if _np_use_nch_log_backend(machine_display):
        nu, npw = _np_backend_env_cred()
        u = (os.environ.get("NCH_BACKEND_USER") or nu).strip()
        p = (os.environ.get("NCH_BACKEND_PASSWORD") or npw).strip()
        return _NCH_BACKEND_BASE, u, p
    if _np_use_backend_osmplay_com(machine_display):
        u = (os.environ.get("CP_BACKEND_USER") or "").strip()
        p = (os.environ.get("CP_BACKEND_PASSWORD") or "").strip()
        return _CP_BACKEND_BASE, u, p
    if _np_use_mdr_log_backend(machine_display):
        u = (os.environ.get("MDR_BACKEND_USER") or _MDR_BACKEND_DEFAULT_USER).strip() or _MDR_BACKEND_DEFAULT_USER
        p = (os.environ.get("MDR_BACKEND_PASSWORD") or _MDR_BACKEND_DEFAULT_PASSWORD).strip() or (
            _MDR_BACKEND_DEFAULT_PASSWORD
        )
        return _MDR_BACKEND_BASE, u, p
    if _np_use_tbr_log_backend(machine_display):
        u = (os.environ.get("TBR_BACKEND_USER") or _TBR_BACKEND_DEFAULT_USER).strip() or _TBR_BACKEND_DEFAULT_USER
        p = (os.environ.get("TBR_BACKEND_PASSWORD") or _TBR_BACKEND_DEFAULT_PASSWORD).strip() or (
            _TBR_BACKEND_DEFAULT_PASSWORD
        )
        return _TBR_BACKEND_BASE, u, p
    if _np_use_tbp_log_backend(machine_display):
        u = (os.environ.get("TBP_BACKEND_USER") or "").strip()
        p = (os.environ.get("TBP_BACKEND_PASSWORD") or "").strip()
        return _TBP_BACKEND_BASE, u, p
    if _np_use_winford_log_backend(machine_display):
        u = (os.environ.get("WF_BACKEND_USER") or "omduty1").strip() or "omduty1"
        p = (os.environ.get("WF_BACKEND_PASSWORD") or "omduty1").strip() or "omduty1"
        return _WINFORD_NP_BASE, u, p
    return NP_BACKEND_DEFAULT_BASE, *_np_backend_env_cred()
