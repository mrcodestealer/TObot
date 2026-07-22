#!/usr/bin/env python3
"""
TObot — Lark bot that indexes ALL emails from the duty mailbox and makes them
searchable from chat.

Mirrored from osedutybot's ``allemail.json`` 1-week email index
(maintenance_mail.py), extended for TObot:

* Stores the email BODY too (osedutybot stores headers only), so ``/search``
  can show who sent the email and what content is inside.
* Rolling retention window (``TOBOT_WINDOW_DAYS``, default 30 days) instead of
  osedutybot's hard weekly reset — this bot is a search archive.

Commands (group chat or P2P):
    /search <email title>   fuzzy search by subject; lists matches with their
                            Message-IDs (the accurate key)
    /search <Message-ID>    exact lookup — shows full details (From/To/Cc/date
                            + body content)
    /search <No.>           open result N from your previous /search listing
    /scan                   force an immediate mailbox re-scan
    /status                 index size, window, last scan time
    /help                   this help

Subscription mode: **persistent connection** (WebSocket via lark-oapi) —
no public Request URL needed. Set it in the Lark developer console:
Event subscriptions → "Receive events through persistent connection".
"""

from __future__ import annotations

import base64
import email
import email.message
import html as html_lib
import http
import imaplib
import json
import os
import re
import ssl
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.utils import getaddresses, parsedate_to_datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_ROOT, ".env"))


def _env(*names: str, default: str = "") -> str:
    """First non-empty env var among ``names`` (lets a copied osedutybot .env work as-is)."""
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v
    return default


# ===================== Lark app =====================
APP_ID = _env("APP_ID")
APP_SECRET = _env("APP_SECRET")
VERIFICATION_TOKEN = _env("VERIFICATION_TOKEN")

# ===================== Mailbox =====================
# MAIL_* preferred; falls back to osedutybot's MAINTENANCE_MAIL_* names so the
# server .env block can be copied straight across.
MAIL_USER = _env("MAIL_USER", "MAINTENANCE_MAIL_USER")
MAIL_PASSWORD = _env("MAIL_PASSWORD", "MAINTENANCE_MAIL_PASSWORD")
MAIL_IMAP_HOST = _env("MAIL_IMAP_HOST", "MAINTENANCE_MAIL_IMAP_HOST", default="imap.larksuite.com")
MAIL_IMAP_PORT = int(_env("MAIL_IMAP_PORT", "MAINTENANCE_MAIL_IMAP_PORT", default="993"))
MAIL_IMAP_SSL = _env("MAIL_IMAP_SSL", "MAINTENANCE_MAIL_IMAP_SSL", default="1").lower() not in (
    "0", "false", "no", "off",
)
MAIL_TZ = _env("TOBOT_TZ", "MAINTENANCE_MAIL_TZ", default="Asia/Manila")

# ===================== Index tuning =====================
STORE_PATH = os.path.join(_ROOT, "allemail.json")
WINDOW_DAYS = min(365, max(1, int(_env("TOBOT_WINDOW_DAYS", default="180"))))
SCAN_INTERVAL_SEC = max(60, int(_env("TOBOT_SCAN_INTERVAL_SEC", default="300")))
SCAN_CAP_PER_FOLDER = min(20000, max(50, int(_env("TOBOT_SCAN_CAP_PER_FOLDER", default="3000"))))
BODY_FETCH_CAP_PER_SCAN = max(20, int(_env("TOBOT_BODY_FETCH_CAP", default="500")))
MAX_ENTRIES = min(100000, max(200, int(_env("TOBOT_MAX_ENTRIES", default="20000"))))
BODY_STORE_MAX_CHARS = max(500, int(_env("TOBOT_BODY_MAX_CHARS", default="6000")))
BODY_SHOW_MAX_CHARS = max(300, int(_env("TOBOT_BODY_SHOW_CHARS", default="3000")))
SEARCH_MAX_RESULTS = max(3, int(_env("TOBOT_SEARCH_MAX_RESULTS", default="10")))
IMAP_TIMEOUT = max(10, int(_env("TOBOT_IMAP_TIMEOUT", default="60")))

_HEADER_FETCH_SPEC = (
    "(BODY.PEEK[HEADER.FIELDS (DATE SUBJECT FROM TO CC MESSAGE-ID)])"
)


def _default_folders() -> list[str]:
    raw = _env("TOBOT_IMAP_FOLDERS", "ALLEMAIL_IMAP_FOLDERS", "JENKINS_REPLY_IMAP_FOLDERS",
               default="INBOX,Sent")
    seen: set[str] = set()
    out: list[str] = []
    for f in raw.split(","):
        name = f.strip()
        if name and name.casefold() not in seen:
            seen.add(name.casefold())
            out.append(name)
    return out or ["INBOX"]


IMAP_FOLDERS = _default_folders()

_store_lock = threading.Lock()
_scan_lock = threading.Lock()
_last_scan_info: dict[str, Any] = {
    "when": "", "scanned": 0, "new_bodies": 0, "error": "",
    "folders": {}, "duration_sec": 0,
}


# ===================== Lark send helpers =====================
_token_lock = threading.Lock()
_token_cache: dict[str, Any] = {"token": "", "expires_at": 0.0}


def get_tenant_access_token() -> str:
    with _token_lock:
        if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
            return _token_cache["token"]
    try:
        resp = requests.post(
            "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": APP_ID, "app_secret": APP_SECRET},
            timeout=20,
        )
        data = resp.json()
    except Exception as ex:
        print(f"[lark] tenant_access_token failed: {ex!r}", flush=True)
        return ""
    token = str(data.get("tenant_access_token") or "")
    if not token:
        print(f"[lark] tenant_access_token error: {data}", flush=True)
        return ""
    ttl = int(data.get("expire") or 3600)
    with _token_lock:
        _token_cache["token"] = token
        _token_cache["expires_at"] = time.time() + max(60, ttl - 300)
    return token


def reply_text(chat_id: str, message_id: str, text: str) -> None:
    """Quote-reply to the inbound message; falls back to a plain chat send."""
    token = get_tenant_access_token()
    if not token:
        return
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    content = json.dumps({"text": text}, ensure_ascii=False)
    mid = (message_id or "").strip()
    if mid:
        try:
            r = requests.post(
                f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reply",
                headers=headers,
                json={"msg_type": "text", "content": content},
                timeout=20,
            ).json()
            if r.get("code") == 0:
                return
            print(f"[lark] reply failed ({r.get('code')}: {r.get('msg')}) — fallback to send", flush=True)
        except Exception as ex:
            print(f"[lark] reply failed: {ex!r} — fallback to send", flush=True)
    try:
        r = requests.post(
            "https://open.larksuite.com/open-apis/im/v1/messages",
            headers=headers,
            params={"receive_id_type": "chat_id"},
            json={"receive_id": chat_id, "msg_type": "text", "content": content},
            timeout=20,
        ).json()
        if r.get("code") != 0:
            print(f"[lark] send failed: {r}", flush=True)
    except Exception as ex:
        print(f"[lark] send failed: {ex!r}", flush=True)


# ===================== Message reactions (GotIt → Done ack) =====================
# Lark UI tooltip says "GotIt" but the official emoji_type is "Get"
# (same fallback chains osedutybot uses).
_GOTIT_EMOJIS = ("Get", "GotIt", "GOTIT", "LGTM", "OnIt", "CheckMark")
_DONE_EMOJIS = ("DONE", "Done", "CheckMark", "JIAYI")


def add_reaction(message_id: str, candidates: tuple[str, ...]) -> str:
    """Add the first accepted emoji reaction; returns its reaction_id ('' on failure)."""
    mid = (message_id or "").strip()
    token = get_tenant_access_token()
    if not (mid and token):
        return ""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    for emoji in candidates:
        try:
            r = requests.post(
                f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reactions",
                headers=headers,
                json={"reaction_type": {"emoji_type": emoji}},
                timeout=10,
            ).json()
        except Exception as ex:
            print(f"[lark] reaction {emoji} failed: {ex!r}", flush=True)
            continue
        if r.get("code") == 0:
            return str((r.get("data") or {}).get("reaction_id") or "")
        print(f"[lark] reaction {emoji} rejected: {r.get('code')} {r.get('msg')}", flush=True)
    return ""


def remove_reaction(message_id: str, reaction_id: str) -> None:
    mid, rid = (message_id or "").strip(), (reaction_id or "").strip()
    token = get_tenant_access_token()
    if not (mid and rid and token):
        return
    try:
        requests.delete(
            f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reactions/{rid}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
    except Exception as ex:
        print(f"[lark] remove reaction failed: {ex!r}", flush=True)


# ===================== Email parsing =====================
def _decode_hdr(raw: Optional[str]) -> str:
    if not raw:
        return ""
    try:
        return str(make_header(decode_header(raw))).strip()
    except Exception:
        return str(raw).strip()


def _normalize_mid(mid: Optional[str]) -> str:
    return (mid or "").strip().lower().strip("<>")


_TAG_RE = re.compile(r"<[^>]+>")
_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.I | re.S)
_BR_RE = re.compile(r"<\s*(br|/p|/div|/tr|/li|/h[1-6])\s*/?\s*>", re.I)


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    s = _STYLE_RE.sub(" ", html)
    s = _BR_RE.sub("\n", s)
    s = _TAG_RE.sub(" ", s)
    s = html_lib.unescape(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r" ?\n ?", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _decode_part(part: email.message.Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        return ""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except (LookupError, UnicodeDecodeError):
        return payload.decode("utf-8", errors="replace")


def extract_body_text(msg: email.message.Message) -> str:
    """Prefer text/plain; fall back to text/html stripped to text."""
    plain, html = "", ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.is_multipart():
                continue
            disp = str(part.get("Content-Disposition") or "").lower()
            if "attachment" in disp:
                continue
            ctype = part.get_content_type()
            if ctype == "text/plain" and not plain:
                plain = _decode_part(part)
            elif ctype == "text/html" and not html:
                html = _decode_part(part)
    else:
        # Single-part: only decode text/* — a lone binary attachment must not
        # be stored as mojibake "content".
        if msg.get_content_type() == "text/html":
            html = _decode_part(msg)
        elif msg.get_content_maintype() == "text":
            plain = _decode_part(msg)
    text = plain.strip() or _html_to_text(html)
    return text[:BODY_STORE_MAX_CHARS]


def _addr_list(raw: str) -> list[str]:
    return [a for _n, a in getaddresses([raw or ""]) if a and "@" in a]


def message_to_entry(msg: email.message.Message, *, folder: str, uid: str,
                     with_body: bool) -> dict[str, Any]:
    subject = _decode_hdr(msg.get("Subject"))
    from_raw = _decode_hdr(msg.get("From"))
    to_raw = _decode_hdr(msg.get("To"))
    cc_raw = _decode_hdr(msg.get("Cc"))
    mid = (msg.get("Message-ID") or "").strip()
    date_raw = (msg.get("Date") or "").strip()
    ts, date_iso = 0.0, ""
    if date_raw:
        try:
            dt = parsedate_to_datetime(date_raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts = dt.timestamp()
            date_iso = dt.isoformat()
        except Exception:
            pass
    if ts <= 0.0:
        # Scan already filtered to the SINCE window; stamp "now" so the window
        # prune in _save_index doesn't drop a message with a broken Date header.
        now = datetime.now(timezone.utc)
        ts, date_iso = now.timestamp(), now.isoformat()
    entry: dict[str, Any] = {
        "subject": subject,
        "message_id": mid,
        "from_raw": from_raw,
        "from": _addr_list(from_raw),
        "to_raw": to_raw,
        "to": _addr_list(to_raw),
        "cc_raw": cc_raw,
        "cc": _addr_list(cc_raw),
        "date": date_iso,
        "date_ts": ts,
        # When WE first saw it — delayed/resent mail with an ancient Date header
        # stays searchable for a full window instead of being pruned instantly.
        "seen_ts": datetime.now(timezone.utc).timestamp(),
        "folder": folder,
        "uid": uid,
    }
    if with_body:
        try:
            entry["body"] = extract_body_text(msg)
        except Exception as ex:
            print(f"[index] body extract failed ({folder}:{uid}): {ex!r}", flush=True)
            entry["body"] = ""
    return entry


# ===================== Index store (allemail.json) =====================
def entry_key(entry: dict[str, Any]) -> str:
    mid = _normalize_mid(entry.get("message_id"))
    if mid:
        return f"mid:{mid}"
    uid = entry.get("uid") or ""
    if uid:
        return f"loc:{(entry.get('folder') or '').casefold()}:{uid}"
    subj = (entry.get("subject") or "").strip().casefold()
    return f"sub:{subj}:{int(float(entry.get('date_ts') or 0.0))}"


def _load_index() -> dict[str, Any]:
    try:
        with open(STORE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("emails"), list):
            return data
    except FileNotFoundError:
        pass
    except Exception as ex:
        print(f"[index] load failed ({ex!r}) — starting empty", flush=True)
    return {"version": 1, "updated_at": "", "emails": []}


def _window_cutoff_ts() -> float:
    """Day-granular cutoff with 1 day of slack past the IMAP SINCE date.

    SINCE compares the server's day-granular INTERNALDATE while we prune on the
    Date header; a second-precision cutoff at exactly now-WINDOW_DAYS would
    prune boundary-day emails the next scan re-fetches — forever.
    """
    d = datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS + 1)
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()


def _entry_alive_ts(e: dict[str, Any]) -> float:
    return max(float(e.get("date_ts") or 0.0), float(e.get("seen_ts") or 0.0))


def _save_index(emails: list[dict[str, Any]]) -> None:
    cutoff = _window_cutoff_ts()
    fresh = [e for e in emails if _entry_alive_ts(e) >= cutoff]
    fresh.sort(key=lambda e: float(e.get("date_ts") or 0.0))
    if len(fresh) > MAX_ENTRIES:
        fresh = fresh[-MAX_ENTRIES:]
    data = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": WINDOW_DAYS,
        "count": len(fresh),
        "emails": fresh,
    }
    tmp = f"{STORE_PATH}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        os.replace(tmp, STORE_PATH)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


def _merge_and_save(new_entries: list[dict[str, Any]]) -> None:
    with _store_lock:
        merged: dict[str, dict[str, Any]] = {}
        for e in _load_index().get("emails", []):
            merged[entry_key(e)] = e
        for e in new_entries:
            key = entry_key(e)
            prev = merged.get(key)
            if prev is None:
                merged[key] = e
                continue
            # Newest wins, but never lose an already-fetched body. "body" key
            # present = body was fetched (even if it extracted to "" — e.g.
            # attachment-only mail); key absent = header-only entry.
            if float(e.get("date_ts") or 0.0) >= float(prev.get("date_ts") or 0.0):
                if "body" not in e and "body" in prev:
                    e = {**e, "body": prev["body"]}
                merged[key] = e
            elif "body" in e and "body" not in prev:
                prev["body"] = e["body"]
        _save_index(list(merged.values()))


# ===================== IMAP scan =====================
def _imap_mailbox_name(folder: str) -> str:
    name = (folder or "").strip() or "INBOX"
    if re.search(r'[\s"\']', name):
        return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return name


class ImapStaleConnectionError(OSError):
    """IMAP socket/SSL dead or hung — abort the scan, reconnect next cycle."""


def _imap_connection_broken(ex: BaseException) -> bool:
    msg = repr(ex).lower()
    needles = (
        "ssl", "tls", "eof", "bad_length", "connection reset", "broken pipe",
        "timed out", "socket error", "connection closed", "unexpected eof",
    )
    return any(n in msg for n in needles)


def _connect_imap() -> imaplib.IMAP4:
    ctx = ssl.create_default_context()
    if MAIL_IMAP_SSL:
        mail = imaplib.IMAP4_SSL(
            MAIL_IMAP_HOST, MAIL_IMAP_PORT, timeout=IMAP_TIMEOUT, ssl_context=ctx
        )
    else:
        mail = imaplib.IMAP4(MAIL_IMAP_HOST, MAIL_IMAP_PORT, timeout=IMAP_TIMEOUT)
        try:
            mail.starttls(ssl_context=ctx)
        except Exception as ex:
            print(f"[scan] STARTTLS unavailable ({ex!r}) — continuing in plaintext", flush=True)
    mail.login(MAIL_USER, MAIL_PASSWORD)
    return mail


def _since_date() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).strftime("%d-%b-%Y")


def _uid_search(mail: imaplib.IMAP4, criteria: str) -> list[bytes]:
    try:
        typ, data = mail.uid("search", None, criteria)
    except Exception as ex:
        if _imap_connection_broken(ex):
            raise ImapStaleConnectionError(f"connection lost during SEARCH: {ex!r}") from ex
        print(f"[scan] UID SEARCH failed: {ex!r}", flush=True)
        return []
    if typ != "OK" or not data or not data[0]:
        return []
    return data[0].split()


def _parse_uid_fetch(data: list) -> dict[bytes, bytes]:
    """FETCH response → {uid: raw_bytes} (headers or full messages).

    Port of osedutybot's ``_parse_uid_header_fetch_data``: the Lark IMAP server
    can echo ``UID`` BEFORE the literal (in the tuple meta) or AFTER it (in the
    trailing bytes element right after the tuple) — handle both, plus inline
    non-literal responses.
    """
    out: dict[bytes, bytes] = {}
    i = 0
    while i < len(data or []):
        item = data[i]
        if isinstance(item, tuple) and len(item) >= 2:
            meta, payload = item[0], item[1]
            if isinstance(meta, bytes) and isinstance(payload, bytes):
                uid_b: Optional[bytes] = None
                m = re.search(rb"UID (\d+)", meta)
                if m:
                    uid_b = m.group(1)
                elif i + 1 < len(data) and isinstance(data[i + 1], bytes):
                    m2 = re.search(rb"UID (\d+)", data[i + 1])
                    if m2:
                        uid_b = m2.group(1)
                        i += 1
                if uid_b:
                    out[uid_b] = payload
            i += 1
            continue
        if isinstance(item, bytes):
            m = re.match(rb"(\d+) \(UID (\d+)", item)
            if m:
                uid_b = m.group(2)
                i += 1
                hdr = b""
                if i < len(data) and isinstance(data[i], bytes):
                    nxt = data[i]
                    if not nxt.startswith(b")") and not re.match(rb"\d+ \(UID ", nxt):
                        hdr = nxt
                        i += 1
                if hdr:
                    out[uid_b] = hdr
                continue
        i += 1
    return out


def _imap_list_folder_names(mail: imaplib.IMAP4) -> list[str]:
    """Actual mailbox names on the server (LIST), for resolving UI labels."""
    names: list[str] = []
    try:
        typ, data = mail.list()
    except Exception as ex:
        if _imap_connection_broken(ex):
            raise ImapStaleConnectionError(f"connection lost during LIST: {ex!r}") from ex
        print(f"[scan] LIST failed: {ex!r}", flush=True)
        return names
    if typ != "OK" or not data:
        return names
    for item in data:
        if not item:
            continue
        line = item.decode("utf-8", errors="replace") if isinstance(item, bytes) else str(item)
        m = re.search(r'"([^"]+)"\s*$', line)
        if m:
            names.append(m.group(1))
        else:
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                names.append(parts[1].strip().strip('"'))
    return names


def _match_folder_candidates(want: str, names: list[str]) -> list[str]:
    """Server folder names that could be the configured label, best match first."""
    want_cf = (want or "").casefold()
    want_flat = want_cf.replace(" ", "")
    out: list[str] = []
    for name in names:
        if name.casefold() == want_cf and name not in out:
            out.append(name)
    for name in names:
        if name.casefold().replace(" ", "") == want_flat and name not in out:
            out.append(name)
    for name in names:
        nc = name.casefold()
        if (want_cf in nc or nc in want_cf) and name not in out:
            out.append(name)
    return out


_folder_resolve_cache: dict[str, str] = {}


def _try_select(mail: imaplib.IMAP4, name: str) -> bool:
    try:
        typ, _ = mail.select(_imap_mailbox_name(name), readonly=True)
    except Exception as ex:
        if _imap_connection_broken(ex):
            raise ImapStaleConnectionError(f"connection lost during SELECT: {ex!r}") from ex
        print(f"[scan] SELECT {name!r} failed: {ex!r}", flush=True)
        return False
    return typ == "OK"


def _select_folder_resolved(mail: imaplib.IMAP4, folder: str) -> str:
    """SELECT the folder, resolving the real mailbox name via LIST if the
    configured label doesn't match exactly (Lark IMAP folder names can differ
    from the UI label). Returns the selected name, or '' if nothing worked."""
    cached = _folder_resolve_cache.get(folder.casefold())
    for name in ([cached] if cached else []) + [folder]:
        if _try_select(mail, name):
            _folder_resolve_cache[folder.casefold()] = name
            return name
    names = _imap_list_folder_names(mail)
    for name in _match_folder_candidates(folder, names):
        if name != folder and _try_select(mail, name):
            print(f"[scan] resolved folder {folder!r} -> {name!r}", flush=True)
            _folder_resolve_cache[folder.casefold()] = name
            return name
    if names:
        print(f"[scan] folder {folder!r} not found; server has: {names}", flush=True)
    return ""


def _scan_folder(mail: imaplib.IMAP4, folder: str,
                 known_keys: set[str], body_budget: list[int]) -> list[dict[str, Any]]:
    selected = _select_folder_resolved(mail, folder)
    if not selected:
        print(f"[scan] SELECT {folder!r} not OK — skipped", flush=True)
        return []
    folder = selected
    uids = _uid_search(mail, f"(SINCE {_since_date()})")
    if not uids:
        return []
    if len(uids) > SCAN_CAP_PER_FOLDER:
        uids = uids[-SCAN_CAP_PER_FOLDER:]

    # Pass 1 — cheap header fetch for everything in the window.
    headers: dict[bytes, dict[str, Any]] = {}
    chunk = 50
    for off in range(0, len(uids), chunk):
        part = uids[off:off + chunk]
        uid_str = b",".join(part).decode()
        try:
            typ, data = mail.uid("fetch", uid_str, _HEADER_FETCH_SPEC)
        except Exception as ex:
            if _imap_connection_broken(ex):
                raise ImapStaleConnectionError(f"connection lost during header fetch: {ex!r}") from ex
            print(f"[scan] header fetch failed in {folder!r}: {ex!r}", flush=True)
            continue
        if typ != "OK" or not data:
            continue
        for uid_b, raw in _parse_uid_fetch(data).items():
            try:
                msg = email.message_from_bytes(raw)
                headers[uid_b] = message_to_entry(
                    msg, folder=folder, uid=uid_b.decode(errors="replace"), with_body=False
                )
            except Exception:
                continue

    # Pass 2 — full body fetch only for messages whose body was never fetched
    # ("body" key absent in the index). Newest first, so during a cold-start
    # backfill the emails people actually /search get their bodies first.
    # The budget caps attempts per scan but is only spent on SUCCESSFUL
    # fetches — a failed chunk gets retried next scan instead of burning cap.
    need_body: list[bytes] = []
    for uid_b in reversed(headers):
        if len(need_body) >= body_budget[0]:
            break
        if entry_key(headers[uid_b]) not in known_keys:
            need_body.append(uid_b)
    entries: list[dict[str, Any]] = []
    fetched_body_uids: set[bytes] = set()
    chunk = 10
    for off in range(0, len(need_body), chunk):
        part = need_body[off:off + chunk]
        uid_str = b",".join(part).decode()
        try:
            typ, data = mail.uid("fetch", uid_str, "(BODY.PEEK[])")
        except Exception as ex:
            if _imap_connection_broken(ex):
                raise ImapStaleConnectionError(f"connection lost during body fetch: {ex!r}") from ex
            print(f"[scan] body fetch failed in {folder!r}: {ex!r}", flush=True)
            continue
        if typ != "OK" or not data:
            continue
        for uid_b, raw in _parse_uid_fetch(data).items():
            try:
                msg = email.message_from_bytes(raw)
                entries.append(message_to_entry(
                    msg, folder=folder, uid=uid_b.decode(errors="replace"), with_body=True
                ))
                fetched_body_uids.add(uid_b)
            except Exception:
                continue
    body_budget[0] -= len(fetched_body_uids)
    for uid_b, entry in headers.items():
        if uid_b not in fetched_body_uids:
            entries.append(entry)
    # Only body-carrying entries count as "known" — header-only ones must stay
    # eligible for a body fetch on the next scan / in a later folder.
    for e in entries:
        if "body" in e:
            known_keys.add(entry_key(e))
    return entries


def scan_mailbox() -> tuple[int, int]:
    """One full scan of all folders. Returns (entries_seen, new_bodies_fetched)."""
    if not (MAIL_USER and MAIL_PASSWORD):
        raise RuntimeError("MAIL_USER / MAIL_PASSWORD not set in .env")
    with _scan_lock:
        with _store_lock:
            known_keys = {
                entry_key(e)
                for e in _load_index().get("emails", [])
                if "body" in e  # body fetched (even if empty text) → skip
            }
        body_budget = [BODY_FETCH_CAP_PER_SCAN]
        all_entries: list[dict[str, Any]] = []
        folder_stats: dict[str, Any] = {}
        started = time.monotonic()
        mail = _connect_imap()
        try:
            for folder in IMAP_FOLDERS:
                try:
                    got = _scan_folder(mail, folder, known_keys, body_budget)
                    all_entries.extend(got)
                    folder_stats[folder] = len(got)
                except ImapStaleConnectionError as ex:
                    # Dead/hung socket: every further op would block IMAP_TIMEOUT
                    # while holding _scan_lock — abort, keep what we got,
                    # reconnect fresh next cycle.
                    print(f"[scan] {ex} — aborting scan after {folder!r}", flush=True)
                    folder_stats[folder] = "connection lost"
                    break
                except Exception as ex:
                    print(f"[scan] folder {folder!r} failed: {ex!r}", flush=True)
                    folder_stats[folder] = f"failed: {ex!r}"
        finally:
            try:
                mail.logout()
            except Exception:
                pass
        _merge_and_save(all_entries)
        new_bodies = BODY_FETCH_CAP_PER_SCAN - body_budget[0]
        _last_scan_info.update({
            "when": datetime.now(timezone.utc).isoformat(),
            "scanned": len(all_entries),
            "new_bodies": new_bodies,
            "error": "",
            "folders": folder_stats,
            "duration_sec": int(time.monotonic() - started),
        })
        return len(all_entries), new_bodies


def _scanner_daemon() -> None:
    while True:
        try:
            seen, new_bodies = scan_mailbox()
            print(f"[scan] ok — {seen} in window, {new_bodies} new bodies fetched", flush=True)
        except Exception as ex:
            _last_scan_info["error"] = repr(ex)
            print(f"[scan] failed: {ex!r}", flush=True)
        time.sleep(SCAN_INTERVAL_SEC)


# ===================== Search =====================
_last_results_lock = threading.Lock()
_last_results: dict[str, list[str]] = {}  # chat_id -> [entry_key, ...] of last listing


def _local_tz() -> Any:
    try:
        return ZoneInfo(MAIL_TZ)
    except Exception:
        return timezone.utc


def _fmt_date(entry: dict[str, Any]) -> str:
    ts = float(entry.get("date_ts") or 0.0)
    if ts <= 0:
        return entry.get("date") or "?"
    return datetime.fromtimestamp(ts, _local_tz()).strftime("%Y-%m-%d %H:%M")


def _score_subject(subject: str, query: str) -> int:
    s = (subject or "").casefold().strip()
    q = (query or "").casefold().strip()
    if not s or not q:
        return 0
    if s == q:
        return 100
    if q in s:
        return 85
    q_tokens = [w for w in re.split(r"\W+", q) if w]
    if not q_tokens:
        return 0
    s_tokens = set(w for w in re.split(r"\W+", s) if w)
    exact = sum(1 for w in q_tokens if w in s_tokens)
    partial = sum(1 for w in q_tokens if any(w in t for t in s_tokens))
    if exact == len(q_tokens):
        return 70
    if partial == len(q_tokens):
        return 55
    if partial == 0:
        return 0
    return int(45 * partial / len(q_tokens))


def _search_entries(query: str) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    """(exact_message_id_hit, scored subject matches newest-first)."""
    with _store_lock:
        emails = list(_load_index().get("emails", []))
    qmid = _normalize_mid(query)
    if qmid:
        for e in reversed(emails):  # newest copy wins
            if _normalize_mid(e.get("message_id")) == qmid:
                return e, []
    scored: list[tuple[int, float, dict[str, Any]]] = []
    for e in emails:
        sc = _score_subject(e.get("subject") or "", query)
        if sc >= 30:
            scored.append((sc, float(e.get("date_ts") or 0.0), e))
    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    # Dedup by key (same Message-ID seen in several folders).
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for _sc, _ts, e in scored:
        k = entry_key(e)
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
        if len(out) >= SEARCH_MAX_RESULTS:
            break
    return None, out


def _format_details(entry: dict[str, Any]) -> str:
    body = (entry.get("body") or "").strip()
    if len(body) > BODY_SHOW_MAX_CHARS:
        body = body[:BODY_SHOW_MAX_CHARS].rstrip() + "\n… (trimmed)"
    if not body:
        if "body" in entry:
            body = "(this email has no text content — probably attachment-only)"
        else:
            body = "(body not fetched yet — try /scan then /search again)"
    lines = [
        f"📧 {entry.get('subject') or '(no subject)'}",
        "──────────",
        f"From: {entry.get('from_raw') or ', '.join(entry.get('from') or []) or '?'}",
    ]
    if entry.get("to_raw") or entry.get("to"):
        lines.append(f"To: {entry.get('to_raw') or ', '.join(entry.get('to') or [])}")
    if entry.get("cc_raw") or entry.get("cc"):
        lines.append(f"Cc: {entry.get('cc_raw') or ', '.join(entry.get('cc') or [])}")
    lines += [
        f"Date: {_fmt_date(entry)} ({MAIL_TZ})",
        f"Folder: {entry.get('folder') or '?'}",
        f"Message-ID: {entry.get('message_id') or '(none)'}",
        "──────────",
        body,
    ]
    return "\n".join(lines)


def _format_listing(query: str, results: list[dict[str, Any]]) -> str:
    lines = [f"🔍 Search “{query}” — {len(results)} match(es), best first:", ""]
    for i, e in enumerate(results, 1):
        frm = (e.get("from") or [e.get("from_raw") or "?"])
        lines.append(f"{i}. {e.get('subject') or '(no subject)'}")
        lines.append(f"   From: {frm[0]} | {_fmt_date(e)}")
        lines.append(f"   ID: {e.get('message_id') or '(none)'}")
    lines += [
        "",
        "Open one with /search <No.> (e.g. /search 1)",
        "or the accurate way: /search <Message-ID>",
    ]
    return "\n".join(lines)


def _entry_by_key(key: str) -> Optional[dict[str, Any]]:
    with _store_lock:
        for e in reversed(_load_index().get("emails", [])):
            if entry_key(e) == key:
                return e
    return None


def handle_search(chat_id: str, query: str) -> str:
    query = (query or "").strip()
    usage = ("Usage: /search <email title or Message-ID>\n"
             "Example: /search Evolution maintenance\n"
             "Example: /search <abc123@larksuite.com>")
    if not query:
        return usage

    # Quoted query = always a title search (escape hatch for numeric titles).
    force_title = False
    if len(query) >= 2 and query[0] == query[-1] and query[0] in ('"', "'", "“"):
        query = query[1:-1].strip("”").strip()
        force_title = True
        if not query:
            return usage

    # Numeric pick from this chat's previous listing.
    note = ""
    if not force_title and re.fullmatch(r"#?\d{1,3}", query):
        n = int(query.lstrip("#"))
        with _last_results_lock:
            keys = _last_results.get(chat_id) or []
        if 1 <= n <= len(keys):
            entry = _entry_by_key(keys[n - 1])
            if entry:
                return _format_details(entry)
            return f"Result {n} is no longer in the index — /search it again."
        if keys:
            return (f"Pick 1–{len(keys)} from your last search, or /search a new title.\n"
                    f'To search “{query}” as a title instead, quote it: /search "{query}"')
        note = f"(no previous listing in this chat — searching “{query}” as a title)\n\n"

    exact, results = _search_entries(query)
    if exact:
        return _format_details(exact)
    if not results:
        with _store_lock:
            idx_count = len(_load_index().get("emails", []))
        if idx_count == 0:
            msg = (note + "The email index is still empty — the first mailbox scan may "
                   "still be running (the bot just started)")
            if _last_scan_info.get("error"):
                msg += f", and the last scan failed: {_last_scan_info['error']}"
            return msg + ".\nCheck /status, or force a scan with /scan."
        return (note + f"No email found for “{query}” in the last {WINDOW_DAYS} days.\n"
                "Tips: try fewer words from the title, or paste the exact Message-ID.\n"
                "A /scan forces a fresh mailbox re-scan.")
    if len(results) == 1:
        return note + _format_details(results[0])
    with _last_results_lock:
        _last_results[chat_id] = [entry_key(e) for e in results]
    return note + _format_listing(query, results)


# ===================== Command router =====================
HELP_TEXT = (
    "TObot — email search bot 📮\n"
    "──────────\n"
    "/search <email title> — find stored emails by title (lists matches + Message-IDs)\n"
    "/search <Message-ID> — exact lookup, shows sender + full content\n"
    "/search <No.> — open result N from your last search listing\n"
    "@TObot <email title> — same as /search (in P2P just type the title)\n"
    "/scan — force a mailbox re-scan now\n"
    "/status — index size, retention window, last scan\n"
    "/help — this help\n"
    "──────────\n"
    f"Mailbox: {MAIL_USER or '(not set)'} | Folders: {', '.join(IMAP_FOLDERS)}\n"
    f"Retention: last {WINDOW_DAYS} days, re-scan every {SCAN_INTERVAL_SEC // 60} min"
)


def _status_text() -> str:
    with _store_lock:
        data = _load_index()
    n = len(data.get("emails", []))
    upd = data.get("updated_at") or "(never)"
    last = _last_scan_info
    lines = [
        "TObot status 📮",
        f"Indexed emails: {n} (window {WINDOW_DAYS}d, cap {MAX_ENTRIES})",
        f"Index updated: {upd}",
        f"Last scan: {last.get('when') or '(not yet)'} — "
        f"{last.get('scanned', 0)} in window, {last.get('new_bodies', 0)} new bodies, "
        f"{last.get('duration_sec', 0)}s",
    ]
    stats = last.get("folders") or {}
    for folder in IMAP_FOLDERS:
        lines.append(f"  {folder}: {stats.get(folder, '(not scanned)')}")
    if last.get("error"):
        lines.append(f"Last scan error: {last['error']}")
    return "\n".join(lines)


def _do_scan_command(chat_id: str, message_id: str) -> None:
    reply_text(chat_id, message_id, "⏳ Scanning mailbox…")
    try:
        seen, new_bodies = scan_mailbox()
        reply_text(chat_id, message_id,
                   f"✅ Scan done — {seen} emails in the {WINDOW_DAYS}-day window, "
                   f"{new_bodies} new bodies fetched.")
    except Exception as ex:
        reply_text(chat_id, message_id, f"❌ Scan failed: {ex!r}")


def _process_message(text: str, chat_id: str, message_id: str, directed: bool) -> None:
    """Pick the action for a message; ack with GotIt while working, Done after.

    ``directed`` = P2P chat or the bot was @-mentioned. Undirected group chatter
    only triggers on TObot's own slash commands, and unknown /commands stay
    silent (other bots share these groups) — so reactions never fire on
    messages we don't answer.
    """
    t = (text or "").strip()
    if not t:
        return
    low = t.lower()
    action = None
    if low.startswith("/search"):
        action = lambda: reply_text(chat_id, message_id, handle_search(chat_id, t[len("/search"):]))
    elif low.startswith("/scan"):
        action = lambda: _do_scan_command(chat_id, message_id)
    elif low.startswith("/status"):
        action = lambda: reply_text(chat_id, message_id, _status_text())
    elif low.startswith("/help"):
        action = lambda: reply_text(chat_id, message_id, HELP_TEXT)
    elif directed:
        if low in ("help", "hi", "hello"):
            action = lambda: reply_text(chat_id, message_id, HELP_TEXT)
        elif not low.startswith("/"):
            # Tagged (or P2P) plain text = search query.
            action = lambda: reply_text(chat_id, message_id, handle_search(chat_id, t))
    if action is None:
        return
    gotit_id = add_reaction(message_id, _GOTIT_EMOJIS)
    try:
        action()
    except Exception as ex:
        print(f"[tobot] command failed: {ex!r}", flush=True)
        try:
            reply_text(chat_id, message_id, f"❌ Something went wrong: {ex!r}")
        except Exception:
            pass
    finally:
        if gotit_id:
            remove_reaction(message_id, gotit_id)
        add_reaction(message_id, _DONE_EMOJIS)


# ===================== Lark persistent connection =====================
_seen_message_ids: OrderedDict[str, float] = OrderedDict()
_seen_lock = threading.Lock()


def _already_handled(message_id: str) -> bool:
    if not message_id:
        return False
    with _seen_lock:
        if message_id in _seen_message_ids:
            return True
        _seen_message_ids[message_id] = time.time()
        while len(_seen_message_ids) > 500:
            _seen_message_ids.popitem(last=False)
    return False


_MENTION_RE = re.compile(r"@_user_\d+\s*")
_bot_open_id_lock = threading.Lock()
_bot_open_id_cache = ""


def _bot_open_id() -> str:
    """This bot's own open_id (GET /bot/v3/info), cached after first success."""
    global _bot_open_id_cache
    with _bot_open_id_lock:
        if _bot_open_id_cache:
            return _bot_open_id_cache
        token = get_tenant_access_token()
        if not token:
            return ""
        try:
            r = requests.get(
                "https://open.larksuite.com/open-apis/bot/v3/info",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            ).json()
            oid = str(((r.get("bot") or (r.get("data") or {}).get("bot") or {})
                       ).get("open_id") or "")
            if oid:
                _bot_open_id_cache = oid
        except Exception as ex:
            print(f"[lark] bot info failed: {ex!r}", flush=True)
        return _bot_open_id_cache


def _bot_mentioned(msg) -> bool:
    mentions = getattr(msg, "mentions", None) or []
    if not mentions:
        return False
    bot_oid = _bot_open_id()
    for m in mentions:
        oid = getattr(getattr(m, "id", None), "open_id", "") or ""
        if bot_oid and oid == bot_oid:
            return True
    # Bot id unknown (info call failed): treat any mention as directed.
    return not bot_oid


def _on_message(data) -> None:
    try:
        msg = data.event.message
        message_id = getattr(msg, "message_id", "") or ""
        if _already_handled(message_id):
            return
        if (getattr(msg, "message_type", "") or "") != "text":
            return
        chat_id = getattr(msg, "chat_id", "") or ""
        chat_type = (getattr(msg, "chat_type", "") or "").lower()
        try:
            content = json.loads(getattr(msg, "content", "") or "{}")
        except (ValueError, TypeError):
            return
        text = _MENTION_RE.sub("", str(content.get("text") or "")).strip()
        if not text:
            return
        directed = chat_type == "p2p" or _bot_mentioned(msg)
        threading.Thread(
            target=_process_message, args=(text, chat_id, message_id, directed), daemon=True
        ).start()
    except Exception as ex:
        print(f"[lark-ws] on_message failed: {ex!r}", flush=True)


# The app may be subscribed (in the developer console) to event types this bot
# never registered — task.task.*, vc.meeting.*, … The stock SDK NACKs those with
# 500, so Lark redelivers them forever and the journal floods with
# "handle message failed … processor not found" (same issue osedutybot patched).
# We ACK 200 and log each distinct unhandled type ONCE. The permanent fix is to
# unsubscribe those events in the console.
_ws_unhandled_seen: set[str] = set()
_ws_unhandled_lock = threading.Lock()
_WS_UNHANDLED_TYPE_RE = re.compile(r"type:\s*(\S+)")


def _ws_unhandled_event_type(exc: Exception) -> Optional[str]:
    msg = str(exc or "")
    if "processor not found" not in msg:
        return None
    m = _WS_UNHANDLED_TYPE_RE.search(msg)
    return m.group(1) if m else "<unknown>"


def _ws_note_unhandled(event_type: str) -> None:
    with _ws_unhandled_lock:
        if event_type in _ws_unhandled_seen:
            return
        _ws_unhandled_seen.add(event_type)
    print(
        f"[lark-ws] ignoring unsubscribed event type {event_type!r} (ACK 200, "
        "silenced). Unsubscribe it in the Lark developer console to stop delivery.",
        flush=True,
    )


def _ws_handler_dispatch(handler, payload: bytes) -> Any:
    for name in ("_do_without_validation", "do_without_validation"):
        fn = getattr(handler, name, None)
        if callable(fn):
            return fn(payload)
    raise RuntimeError("lark-oapi EventDispatcherHandler has no dispatch method")


def _apply_ws_unhandled_patch() -> None:
    """Port of osedutybot's ws-client patch: ACK unhandled event types."""
    from lark_oapi.core.const import UTF_8
    from lark_oapi.core.json import JSON
    from lark_oapi.ws.client import Client, _get_by_key
    from lark_oapi.ws.const import (
        HEADER_BIZ_RT, HEADER_MESSAGE_ID, HEADER_SEQ, HEADER_SUM,
        HEADER_TRACE_ID, HEADER_TYPE,
    )
    from lark_oapi.ws.enum import MessageType
    from lark_oapi.ws.model import Response

    if getattr(Client, "_tobot_unhandled_patch", False):
        return

    async def _handle_data_frame_patched(self, frame):
        hs = frame.headers
        msg_id = _get_by_key(hs, HEADER_MESSAGE_ID)
        trace_id = _get_by_key(hs, HEADER_TRACE_ID)
        sum_ = _get_by_key(hs, HEADER_SUM)
        seq = _get_by_key(hs, HEADER_SEQ)
        type_ = _get_by_key(hs, HEADER_TYPE)

        pl = frame.payload
        if int(sum_) > 1:
            pl = self._combine(msg_id, int(sum_), int(seq), pl)
            if pl is None:
                return

        message_type = MessageType(type_)
        resp = Response(code=http.HTTPStatus.OK)
        try:
            start = int(round(time.time() * 1000))
            if message_type in (MessageType.EVENT, MessageType.CARD):
                result = _ws_handler_dispatch(self._event_handler, pl)
            else:
                return
            end = int(round(time.time() * 1000))
            header = hs.add()
            header.key = HEADER_BIZ_RT
            header.value = str(end - start)
            if result is not None:
                resp.data = base64.b64encode(JSON.marshal(result).encode(UTF_8))
        except Exception as e:
            unhandled = _ws_unhandled_event_type(e)
            if unhandled is not None:
                _ws_note_unhandled(unhandled)
            else:
                from lark_oapi.core.log import logger

                logger.error(
                    self._fmt_log(
                        "handle message failed, message_type: {}, message_id: {}, trace_id: {}, err: {}",
                        message_type.value, msg_id, trace_id, e,
                    )
                )
                resp = Response(code=http.HTTPStatus.INTERNAL_SERVER_ERROR)

        frame.payload = JSON.marshal(resp).encode(UTF_8)
        await self._write_message(frame.SerializeToString())

    Client._handle_data_frame = _handle_data_frame_patched
    Client._tobot_unhandled_patch = True
    print("[lark-ws] patched ws client: unhandled event types are ACKed silently", flush=True)


def run_ws_forever() -> None:
    import lark_oapi as lark

    if not (APP_ID and APP_SECRET):
        raise RuntimeError("Set APP_ID and APP_SECRET in .env")
    try:
        _apply_ws_unhandled_patch()
    except Exception as ex:
        print(f"[lark-ws] unhandled-event patch not applied ({ex!r}) — "
              "SDK will log unsubscribed events noisily", flush=True)
    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_on_message)
        .build()
    )
    domain_name = _env("LARK_DOMAIN", default="lark").lower()
    domain = lark.FEISHU_DOMAIN if domain_name == "feishu" else lark.LARK_DOMAIN
    while True:
        try:
            cli = lark.ws.Client(
                APP_ID,
                APP_SECRET,
                event_handler=handler,
                log_level=lark.LogLevel.INFO,
                domain=domain,
            )
            print(
                "[lark-ws] persistent connection starting "
                "(console: Events → receive through persistent connection)",
                flush=True,
            )
            cli.start()
            print("[lark-ws] client returned — reconnecting in 15s", flush=True)
        except Exception as ex:
            print(f"[lark-ws] crashed: {ex!r} — reconnecting in 15s", flush=True)
        time.sleep(15)


def main() -> int:
    os.chdir(_ROOT)
    missing = [n for n, v in (
        ("APP_ID", APP_ID), ("APP_SECRET", APP_SECRET),
        ("MAIL_USER", MAIL_USER), ("MAIL_PASSWORD", MAIL_PASSWORD),
    ) if not v]
    if missing:
        print(f"[tobot] missing .env values: {', '.join(missing)}", flush=True)
        return 1
    print(
        f"[tobot] mailbox={MAIL_USER} imap={MAIL_IMAP_HOST}:{MAIL_IMAP_PORT} "
        f"folders={IMAP_FOLDERS} window={WINDOW_DAYS}d interval={SCAN_INTERVAL_SEC}s",
        flush=True,
    )
    threading.Thread(target=_scanner_daemon, daemon=True, name="scanner").start()
    run_ws_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
