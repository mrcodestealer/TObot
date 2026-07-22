#!/usr/bin/env python3
"""
TObot — Lark bot that indexes ALL emails from the duty mailbox and makes them
searchable from chat.

Mirrored from osedutybot's ``allemail.json`` 1-week email index
(maintenance_mail.py), adapted for TObot:

* The index tracks TITLE → MESSAGE-ID only (plus From/To/date/folder/uid).
  ``/search <title>`` resolves the title to its Message-ID, then uses that ID
  to retrieve the exact email's content LIVE from the mailbox at view time
  (in-memory cache only — nothing persisted).
* Rolling retention window (``TOBOT_WINDOW_DAYS``, default 180 days) instead
  of osedutybot's hard weekly reset — this bot is a search archive.

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
MAX_ENTRIES = min(100000, max(200, int(_env("TOBOT_MAX_ENTRIES", default="20000"))))
# The index stores ONLY titles + Message-IDs (+ From/To/date/folder/uid).
# Email content is fetched live from IMAP at view time; these caps bound that
# runtime fetch, nothing is persisted.
BODY_STORE_MAX_CHARS = max(500, int(_env("TOBOT_BODY_MAX_CHARS", default="40000")))
# Text-fallback trim only — cards always show the full fetched body (paginated).
BODY_SHOW_MAX_CHARS = max(300, int(_env("TOBOT_BODY_SHOW_CHARS", default="3000")))
# Body/meta chars per card — stays safely under Lark's ~30KB card limit.
CARD_CHARS_BUDGET = max(4000, int(_env("TOBOT_CARD_CHARS", default="18000")))
# Recently fetched contents kept in memory so re-opening an email is instant.
BODY_CACHE_MAX = max(20, int(_env("TOBOT_BODY_CACHE_MAX", default="300")))
SEARCH_MAX_RESULTS = max(3, int(_env("TOBOT_SEARCH_MAX_RESULTS", default="10")))
# Exact-title / thread mode shows the WHOLE conversation, so it gets a much
# larger cap than the fuzzy picker listing (which is just a menu).
THREAD_MAX_RESULTS = max(SEARCH_MAX_RESULTS, int(_env("TOBOT_THREAD_MAX_RESULTS", default="60")))
IMAP_TIMEOUT = max(10, int(_env("TOBOT_IMAP_TIMEOUT", default="60")))

_HEADER_FETCH_SPEC = (
    "(BODY.PEEK[HEADER.FIELDS (DATE SUBJECT FROM TO CC MESSAGE-ID)])"
)


def _default_folders() -> list[str]:
    raw = _env("TOBOT_IMAP_FOLDERS", "ALLEMAIL_IMAP_FOLDERS", "JENKINS_REPLY_IMAP_FOLDERS",
               default="*")
    if raw.strip() in ("*", "ALL", "all"):
        return ["*"]
    seen: set[str] = set()
    out: list[str] = []
    for f in raw.split(","):
        name = f.strip()
        if name and name.casefold() not in seen:
            seen.add(name.casefold())
            out.append(name)
    return out or ["INBOX"]


IMAP_FOLDERS = _default_folders()
SCAN_ALL_FOLDERS = IMAP_FOLDERS == ["*"]
# Folders skipped in "*" mode (matched against the decoded display name).
IMAP_EXCLUDE = {
    f.strip().casefold()
    for f in _env("TOBOT_IMAP_EXCLUDE", default="Spam,Trash,Drafts,Junk,Deleted Messages").split(",")
    if f.strip()
}


def _folders_label() -> str:
    if SCAN_ALL_FOLDERS:
        return f"ALL folders (except {', '.join(sorted(IMAP_EXCLUDE))})"
    return ", ".join(IMAP_FOLDERS)

_store_lock = threading.Lock()
_scan_lock = threading.Lock()
_last_scan_info: dict[str, Any] = {
    "when": "", "scanned": 0, "error": "",
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


def _reply(chat_id: str, message_id: str, msg_type: str, content: str) -> bool:
    """Quote-reply to the inbound message; falls back to a plain chat send."""
    token = get_tenant_access_token()
    if not token:
        return False
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    mid = (message_id or "").strip()
    if mid:
        try:
            r = requests.post(
                f"https://open.larksuite.com/open-apis/im/v1/messages/{mid}/reply",
                headers=headers,
                json={"msg_type": msg_type, "content": content},
                timeout=20,
            ).json()
            if r.get("code") == 0:
                return True
            print(f"[lark] reply failed ({r.get('code')}: {r.get('msg')}) — fallback to send", flush=True)
        except Exception as ex:
            print(f"[lark] reply failed: {ex!r} — fallback to send", flush=True)
    try:
        r = requests.post(
            "https://open.larksuite.com/open-apis/im/v1/messages",
            headers=headers,
            params={"receive_id_type": "chat_id"},
            json={"receive_id": chat_id, "msg_type": msg_type, "content": content},
            timeout=20,
        ).json()
        if r.get("code") == 0:
            return True
        print(f"[lark] send failed: {r}", flush=True)
    except Exception as ex:
        print(f"[lark] send failed: {ex!r}", flush=True)
    return False


def reply_text(chat_id: str, message_id: str, text: str) -> bool:
    return _reply(chat_id, message_id, "text", json.dumps({"text": text}, ensure_ascii=False))


def reply_card(chat_id: str, message_id: str, card: dict) -> bool:
    return _reply(chat_id, message_id, "interactive", json.dumps(card, ensure_ascii=False))


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
    return plain.strip() or _html_to_text(html)


def _addr_list(raw: str) -> list[str]:
    return [a for _n, a in getaddresses([raw or ""]) if a and "@" in a]


def message_to_entry(msg: email.message.Message, *, folder: str, uid: str,
                     with_body: bool) -> dict[str, Any]:
    subject = _decode_hdr(msg.get("Subject"))
    from_raw = _decode_hdr(msg.get("From"))
    to_raw = _decode_hdr(msg.get("To"))
    cc_raw = _decode_hdr(msg.get("Cc"))
    # Message-IDs legally contain no whitespace — collapsing it also removes
    # CRLF artifacts from folded headers (they would corrupt IMAP SEARCH).
    mid = re.sub(r"\s+", "", msg.get("Message-ID") or "")
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
            text = extract_body_text(msg)
        except Exception as ex:
            print(f"[index] body extract failed ({folder}:{uid}): {ex!r}", flush=True)
            text = ""
        # body_full also marks the entry as new-format: entries stored before
        # this flag existed may hold a cut body and get one live re-fetch on open.
        entry["body_full"] = len(text) <= BODY_STORE_MAX_CHARS
        entry["body"] = text[:BODY_STORE_MAX_CHARS]
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


# In-memory copy of allemail.json so /search never re-parses the file from
# disk. Disk is read once (startup) and written on save; all reads/writes
# hold _store_lock.
_index_cache: Optional[dict[str, Any]] = None


def _load_index() -> dict[str, Any]:
    global _index_cache
    if _index_cache is not None:
        return _index_cache
    try:
        with open(STORE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("emails"), list):
            _index_cache = data
            return data
    except FileNotFoundError:
        pass
    except Exception as ex:
        print(f"[index] load failed ({ex!r}) — starting empty", flush=True)
    _index_cache = {"version": 1, "updated_at": "", "emails": []}
    return _index_cache


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
    global _index_cache
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
    _index_cache = data
    tmp = f"{STORE_PATH}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, STORE_PATH)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


def _strip_runtime_fields(e: dict[str, Any]) -> dict[str, Any]:
    """Index entries persist headers only — body/body_full live in the runtime cache."""
    if "body" in e or "body_full" in e:
        return {k: v for k, v in e.items() if k not in ("body", "body_full")}
    return e


def _merge_and_save(new_entries: list[dict[str, Any]]) -> None:
    with _store_lock:
        merged: dict[str, dict[str, Any]] = {}
        for e in _load_index().get("emails", []):
            merged[entry_key(e)] = _strip_runtime_fields(e)
        for e in new_entries:
            e = _strip_runtime_fields(e)
            key = entry_key(e)
            prev = merged.get(key)
            # Newest wins per key.
            if prev is None or float(e.get("date_ts") or 0.0) >= float(prev.get("date_ts") or 0.0):
                merged[key] = e
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


def _imap_utf7_decode(name: str) -> str:
    """IMAP modified-UTF-7 → readable text (e.g. 'IGO&jURukFIpdShUaGKl-' → 'IGO资源利用周报')."""
    out: list[str] = []
    i = 0
    while i < len(name):
        ch = name[i]
        if ch != "&":
            out.append(ch)
            i += 1
            continue
        j = name.find("-", i + 1)
        if j < 0:
            out.append(name[i:])
            break
        b64 = name[i + 1:j]
        if not b64:
            out.append("&")
        else:
            try:
                pad = "=" * ((4 - len(b64) % 4) % 4)
                raw = base64.b64decode(b64.replace(",", "/") + pad)
                out.append(raw.decode("utf-16-be"))
            except Exception:
                out.append(name[i:j + 1])
        i = j + 1
    return "".join(out)


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
    """Server folder names (raw) that could be the configured label, best first.

    Matches against both the raw name and its modified-UTF-7 decoded form, so a
    label like "IGO资源利用周报" finds the encoded "IGO&jURukFIpdShUaGKl-".
    """
    want_cf = (want or "").casefold()
    want_flat = want_cf.replace(" ", "")
    forms = [(n, {n.casefold(), _imap_utf7_decode(n).casefold()}) for n in names]
    out: list[str] = []
    for name, cfs in forms:
        if want_cf in cfs and name not in out:
            out.append(name)
    for name, cfs in forms:
        if want_flat in {c.replace(" ", "") for c in cfs} and name not in out:
            out.append(name)
    for name, cfs in forms:
        if any(want_cf in c or c in want_cf for c in cfs) and name not in out:
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
                 raw_name: Optional[str] = None) -> list[dict[str, Any]]:
    """Header-only index scan: title + Message-ID (+ From/To/date/folder/uid).

    Content is never fetched here — /search resolves a title to its Message-ID
    and pulls the email body live from IMAP at view time.
    """
    if raw_name is not None:
        selected = raw_name if _try_select(mail, raw_name) else ""
    else:
        selected = _select_folder_resolved(mail, folder)
    if not selected:
        print(f"[scan] SELECT {folder!r} not OK — skipped", flush=True)
        return []
    folder = _imap_utf7_decode(selected)
    uids = _uid_search(mail, f"(SINCE {_since_date()})")
    if not uids:
        return []
    if len(uids) > SCAN_CAP_PER_FOLDER:
        uids = uids[-SCAN_CAP_PER_FOLDER:]
    entries: list[dict[str, Any]] = []
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
                entries.append(message_to_entry(
                    msg, folder=folder, uid=uid_b.decode(errors="replace"), with_body=False
                ))
            except Exception:
                continue
    return entries


def scan_mailbox() -> int:
    """One header-only scan of all folders. Returns entries seen in window."""
    if not (MAIL_USER and MAIL_PASSWORD):
        raise RuntimeError("MAIL_USER / MAIL_PASSWORD not set in .env")
    with _scan_lock:
        all_entries: list[dict[str, Any]] = []
        folder_stats: dict[str, Any] = {}
        started = time.monotonic()
        _last_scan_info["running_since"] = datetime.now(timezone.utc).isoformat()
        print(f"[scan] starting — folders={_folders_label()}", flush=True)
        mail = _connect_imap()
        try:
            # (display_name, raw_select_name|None) per folder this scan covers.
            if SCAN_ALL_FOLDERS:
                todo = [
                    (_imap_utf7_decode(raw), raw)
                    for raw in _imap_list_folder_names(mail)
                    if _imap_utf7_decode(raw).casefold() not in IMAP_EXCLUDE
                ]
                if not todo:
                    print("[scan] LIST returned no folders — nothing to scan", flush=True)
            else:
                todo = [(f, None) for f in IMAP_FOLDERS]
            for folder, raw_name in todo:
                try:
                    got = _scan_folder(mail, folder, raw_name=raw_name)
                    all_entries.extend(got)
                    folder_stats[folder] = len(got)
                    print(f"[scan] {folder}: {len(got)} in window", flush=True)
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
        _last_scan_info.update({
            "when": datetime.now(timezone.utc).isoformat(),
            "scanned": len(all_entries),
            "error": "",
            "folders": folder_stats,
            "duration_sec": int(time.monotonic() - started),
            "running_since": "",
        })
        return len(all_entries)


# Recently fetched contents — re-opening the same email is instant, and the
# index itself stays headers-only.
_body_cache: OrderedDict[str, tuple[str, bool]] = OrderedDict()
_body_cache_lock = threading.Lock()


def _body_cache_get(key: str) -> Optional[tuple[str, bool]]:
    with _body_cache_lock:
        hit = _body_cache.get(key)
        if hit is not None:
            _body_cache.move_to_end(key)
        return hit


def _body_cache_put(key: str, body: str, full: bool) -> None:
    with _body_cache_lock:
        _body_cache[key] = (body, full)
        _body_cache.move_to_end(key)
        while len(_body_cache) > BODY_CACHE_MAX:
            _body_cache.popitem(last=False)


def _fetch_uid_body(mail: imaplib.IMAP4, uid: str) -> Optional[email.message.Message]:
    try:
        typ, data = mail.uid("fetch", str(uid), "(BODY.PEEK[])")
    except Exception as ex:
        if _imap_connection_broken(ex):
            raise ImapStaleConnectionError(f"connection lost during body fetch: {ex!r}") from ex
        print(f"[live-fetch] uid fetch failed: {ex!r}", flush=True)
        return None
    if typ != "OK" or not data:
        return None
    for _uid_b, raw in _parse_uid_fetch(data).items():
        try:
            return email.message_from_bytes(raw)
        except Exception:
            return None
    return None


# At most this many folders are searched per entry on the Message-ID fallback
# path, so one /search can't turn into hundreds of IMAP round-trips.
LIVE_FETCH_FOLDER_CAP = max(2, int(_env("TOBOT_LIVE_FETCH_FOLDERS", default="6")))


def _search_folders_for_live_fetch(mail: imaplib.IMAP4, first: str) -> list[str]:
    if SCAN_ALL_FOLDERS:
        names = [
            _imap_utf7_decode(raw)
            for raw in _imap_list_folder_names(mail)
            if _imap_utf7_decode(raw).casefold() not in IMAP_EXCLUDE
        ]
    else:
        names = list(IMAP_FOLDERS)
    out = [first] if first else []
    for n in names:
        if n.casefold() != (first or "").casefold():
            out.append(n)
    return out[:LIVE_FETCH_FOLDER_CAP]


def _mid_search_needles(mid: str) -> list[str]:
    """Safe SEARCH needles for a Message-ID: quotes/backslashes removed (they
    corrupt the IMAP quoted string — cf. osedutybot's needle sanitizing), and
    empty needles dropped ('' substring-matches EVERY message)."""
    out: list[str] = []
    for cand in (mid, mid.strip("<>")):
        cand = re.sub(r'[\s"\\]+', "", cand)
        if cand and cand.strip("<>") and cand not in out:
            out.append(cand)
    return out


def _fetch_content_for_entry(mail: imaplib.IMAP4, e: dict[str, Any]) -> Optional[str]:
    """Retrieve one email's content — by stored folder+uid first, then by
    Message-ID search (the accurate key) if the email moved. Either way the
    fetched message's Message-ID must match the entry's before it is shown."""
    mid = (e.get("message_id") or "").strip()
    want = _normalize_mid(mid)
    # Fast path: the folder+uid recorded at scan time.
    if e.get("folder") and e.get("uid"):
        if _select_folder_resolved(mail, e["folder"]):
            msg = _fetch_uid_body(mail, str(e["uid"]))
            if msg is not None:
                fmid = _normalize_mid(msg.get("Message-ID"))
                # uid still points at the right email unless mids disagree
                if not want or not fmid or fmid == want:
                    return extract_body_text(msg)
    # Accurate path: find the email by its Message-ID wherever it lives now.
    # HEADER search is substring-based, so every hit is verified against the
    # exact Message-ID before its content is accepted (and cached upstream).
    needles = _mid_search_needles(mid)
    if want and needles:
        for folder in _search_folders_for_live_fetch(mail, e.get("folder") or ""):
            if not _select_folder_resolved(mail, folder):
                continue
            uids: list[bytes] = []
            for needle in needles:
                uids = _uid_search(mail, f'(HEADER Message-ID "{needle}")')
                if uids:
                    break
            # Newest hits first; substring matches are rejected by the check.
            for uid_b in list(reversed(uids))[:5]:
                msg = _fetch_uid_body(mail, uid_b.decode(errors="replace"))
                if msg is not None and _normalize_mid(msg.get("Message-ID")) == want:
                    return extract_body_text(msg)
    return None


def fetch_contents(entries: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    """Attach live-fetched content to index entries before display.

    The index maps title → Message-ID; this resolves each Message-ID to the
    actual email on the server and retrieves its content. Cached in memory,
    never persisted. Best-effort: failures keep the entry content-less.
    """
    out: dict[str, dict[str, Any]] = {}
    need: list[dict[str, Any]] = []
    for e in entries:
        key = entry_key(e)
        cached = _body_cache_get(key)
        if cached is not None:
            out[key] = {**e, "body": cached[0], "body_full": cached[1]}
        elif len(need) < limit:
            need.append(e)
    if need:
        try:
            mail = _connect_imap()
        except Exception as ex:
            print(f"[live-fetch] connect failed: {ex!r}", flush=True)
            mail = None
        if mail is not None:
            try:
                for e in need:
                    text = _fetch_content_for_entry(mail, e)
                    key = entry_key(e)
                    if text is None:
                        # Attempted but not found anywhere — genuinely missing
                        # (distinct from "never attempted": limit/stale abort).
                        out[key] = {**e, "fetch_missing": True}
                        continue
                    full = len(text) <= BODY_STORE_MAX_CHARS
                    text = text[:BODY_STORE_MAX_CHARS]
                    _body_cache_put(key, text, full)
                    out[key] = {**e, "body": text, "body_full": full}
            except ImapStaleConnectionError as ex:
                print(f"[live-fetch] {ex}", flush=True)
            finally:
                try:
                    mail.logout()
                except Exception:
                    pass
    if not out:
        return entries
    return [out.get(entry_key(e), e) for e in entries]


def _scanner_daemon() -> None:
    print(f"[scan] header-only index (titles + Message-IDs) over the "
          f"{WINDOW_DAYS}-day window — content is fetched live at /search time", flush=True)
    while True:
        try:
            seen = scan_mailbox()
            print(f"[scan] ok — {seen} emails indexed in window", flush=True)
        except Exception as ex:
            _last_scan_info["error"] = repr(ex)
            _last_scan_info["running_since"] = ""
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


# Reply/forward prefixes (incl. Chinese) stripped when comparing thread titles.
_SUBJECT_PREFIX_RE = re.compile(
    r"^\s*(?:(?:re|fw|fwd|aw|回复|回覆|转发|轉發|答复|答覆)\s*(?:\[\d+\])?\s*[::])\s*", re.I
)


def _base_subject(s: str) -> str:
    """Whitespace-normalized subject with all Re:/Fwd: prefixes stripped."""
    s = re.sub(r"\s+", " ", (s or "").strip())
    while True:
        t = _SUBJECT_PREFIX_RE.sub("", s)
        if t == s:
            break
        s = t
    return s.casefold()


def _score_subject(subject: str, query: str) -> int:
    # Whitespace-normalized so pasted titles (line wraps, collapsed double
    # spaces) still count as an exact match.
    s = re.sub(r"\s+", " ", (subject or "").casefold().strip())
    q = re.sub(r"\s+", " ", (query or "").casefold().strip())
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


def _search_entries(
    query: str,
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]], bool, int]:
    """(exact_message_id_hit, matches newest-first, exact_title, total_matches).

    When the query equals a subject exactly (whitespace-normalized), ONLY the
    copies of that exact title are returned — similar-but-different titles are
    dropped so "search that email only" really means that email.
    """
    with _store_lock:
        emails = list(_load_index().get("emails", []))
    qmid = _normalize_mid(query)
    if qmid:
        for e in reversed(emails):  # newest copy wins
            if _normalize_mid(e.get("message_id")) == qmid:
                return e, [], False, 1
    # Thread-exact: query equals a subject up to Re:/Fwd: prefixes and
    # whitespace — return the WHOLE conversation (original + every reply),
    # oldest first, deduped by Message-ID.
    qbase = _base_subject(query)
    if qbase:
        thread: dict[str, dict[str, Any]] = {}
        for e in emails:
            if _base_subject(e.get("subject") or "") != qbase:
                continue
            k = entry_key(e)
            prev = thread.get(k)
            if prev is None or float(e.get("date_ts") or 0.0) >= float(prev.get("date_ts") or 0.0):
                thread[k] = e
        if thread:
            ordered = sorted(thread.values(), key=lambda e: float(e.get("date_ts") or 0.0))
            total = len(ordered)
            if total > THREAD_MAX_RESULTS:
                ordered = ordered[-THREAD_MAX_RESULTS:]  # keep the newest N
            return None, ordered, True, total
    scored: list[tuple[int, float, dict[str, Any]]] = []
    for e in emails:
        sc = _score_subject(e.get("subject") or "", query)
        if sc >= 30:
            scored.append((sc, float(e.get("date_ts") or 0.0), e))
    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    # Dedup by key (same Message-ID seen in several folders).
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    total = 0
    for _sc, _ts, e in scored:
        k = entry_key(e)
        if k in seen:
            continue
        seen.add(k)
        total += 1
        if len(out) < SEARCH_MAX_RESULTS:
            out.append(e)
    return None, out, False, total


def _format_details(entry: dict[str, Any]) -> str:
    body = _display_body(entry)
    if len(body) > BODY_SHOW_MAX_CHARS:
        body = body[:BODY_SHOW_MAX_CHARS].rstrip() + "\n… (trimmed)"
    if not body:
        if "body" in entry:
            body = "(this email has no text content — probably attachment-only)"
        elif entry.get("fetch_missing"):
            body = "(this email couldn't be found in the mailbox anymore — it may have been deleted)"
        else:
            body = "(content not loaded — /search again to retrieve it)"
    lines = [
        f"📧 {entry.get('subject') or '(no subject)'}",
        "──────────",
        f"From: {entry.get('from_raw') or ', '.join(entry.get('from') or []) or '?'}",
        f"Date: {_fmt_date(entry)} ({MAIL_TZ})",
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


# Markers that start quoted history inside a reply body (top-posting): quoted
# ">" lines, "On … wrote:", Outlook "-----Original Message-----" / underscore
# dividers, and embedded "From:" header blocks of the quoted mail.
_QUOTE_LINE_RE = re.compile(r"^\s*[>＞]")
_QUOTE_CUT_RES = [
    re.compile(r"^\s*-{2,}\s*(Original|Forwarded) Message\s*-{2,}", re.I),
    re.compile(r"^\s*_{10,}\s*$"),
    re.compile(r"^\s*On .{4,160} wrote:\s*$", re.I),
    re.compile(r"^\s*在.{2,120}写道[:：]\s*$"),
    re.compile(r"^\s*发件人[:：]"),
    re.compile(r"^\s*From:\s*\S", re.I),
]
HIDE_QUOTED_HISTORY = _env("TOBOT_HIDE_QUOTES", default="1").lower() not in (
    "0", "false", "no", "off",
)


def strip_quoted_history(text: str) -> str:
    """Drop the quoted previous-messages tail of a reply, keeping the new part.

    Returns "" when the body is nothing but quoted history.
    """
    lines = (text or "").splitlines()
    cut = len(lines)
    for i, ln in enumerate(lines):
        if _QUOTE_LINE_RE.match(ln) or any(rx.match(ln) for rx in _QUOTE_CUT_RES):
            cut = i
            break
    head = lines[:cut]
    # Drop a dangling attribution line ("On …," / "…:") left right above the quote.
    while head and re.search(r"(wrote:|写道[:：]|[:：])\s*$", head[-1].strip()) and cut < len(lines):
        head.pop()
    return "\n".join(head).strip()


def _display_body(e: dict[str, Any]) -> str:
    """The body as shown in cards/fallback: quoted history hidden (config-gated)."""
    body = (e.get("body") or "").strip()
    if not body or not HIDE_QUOTED_HISTORY:
        return body
    stripped = strip_quoted_history(body)
    if stripped == body:
        return body
    if stripped:
        return stripped
    return "(this email only quoted earlier messages — see the previous emails in this thread)"


def _split_body(body: str, budget: int) -> list[str]:
    """Split a long body into chunks ≤ budget, preferring newline boundaries."""
    chunks: list[str] = []
    pos = 0
    while pos < len(body):
        end = pos + budget
        if end < len(body):
            nl = body.rfind("\n", pos + budget // 2, end)
            if nl > pos:
                end = nl
        chunks.append(body[pos:end].strip("\n"))
        pos = end
    return chunks or [""]


def _cards_for_entries(title: str, entries: list[dict[str, Any]],
                       total: Optional[int] = None) -> list[dict[str, Any]]:
    """Lark interactive cards showing the FULL content of every email copy.

    Identical bodies are deduped; whatever doesn't fit under Lark's per-card
    size limit continues in follow-up cards ("card 2/N").
    """
    n = len(entries)
    pages: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    cur_size = 0

    def emit(element: dict[str, Any], size: int) -> None:
        nonlocal cur, cur_size
        if cur and cur_size + size > CARD_CHARS_BUDGET:
            pages.append(cur)
            cur, cur_size = [], 0
        cur.append(element)
        cur_size += size

    if total and total > n:
        note = f"*(showing the newest {n} of {total} emails with this title)*"
        emit({"tag": "markdown", "content": note}, len(note))
    seen_bodies: dict[str, int] = {}
    for i, e in enumerate(entries, 1):
        if i > 1:
            emit({"tag": "hr"}, 20)
        # Keep the card lean: just who sent it, when, and the content.
        meta = []
        if n > 1:
            meta.append(f"**#{i}**")
        meta.append(f"**From:** {e.get('from_raw') or ', '.join(e.get('from') or []) or '?'}")
        meta.append(f"**Date:** {_fmt_date(e)} ({MAIL_TZ})")
        meta_md = "\n".join(meta)
        emit({"tag": "markdown", "content": meta_md}, len(meta_md))
        body = _display_body(e)
        if not body:
            if "body" in e:
                body = "(this email has no text content — probably attachment-only)"
            elif e.get("fetch_missing"):
                body = ("(this email couldn't be found in the mailbox anymore — "
                        "it may have been deleted)")
            else:
                body = "(content not loaded — /search again to retrieve it)"
        else:
            prev = seen_bodies.get(body)
            if prev is not None:
                body = f"(same content as #{prev})"
            else:
                seen_bodies[body] = i
                if not e.get("body_full", True):
                    body += f"\n… (email longer than {BODY_STORE_MAX_CHARS} chars — cut here)"
        parts = _split_body(body, CARD_CHARS_BUDGET)
        for k, piece in enumerate(parts, 1):
            if len(parts) > 1:
                piece = f"*(content part {k}/{len(parts)})*\n" + piece
            emit({"tag": "markdown", "content": piece}, len(piece))
    pages.append(cur)
    cards: list[dict[str, Any]] = []
    for p, elements in enumerate(pages, 1):
        head = f"📧 {title}" if len(pages) == 1 else f"📧 {title} ({p}/{len(pages)})"
        cards.append({
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": head[:150]},
                "template": "blue",
            },
            "elements": elements,
        })
    return cards


def _card_for_entries(title: str, entries: list[dict[str, Any]],
                      total: Optional[int] = None) -> dict[str, Any]:
    """First card only — kept for compatibility (tests/fallbacks)."""
    return _cards_for_entries(title, entries, total)[0]


def handle_search(chat_id: str, query: str) -> tuple[str, Any]:
    """('text', message) or ('card', (title, entries, total)) for a search query."""
    query = (query or "").strip()
    usage = ("Usage: /search <email title or Message-ID>\n"
             "Example: /search Evolution maintenance\n"
             "Example: /search <abc123@larksuite.com>")
    if not query:
        return "text", usage

    # Quoted query = always a title search (escape hatch for numeric titles).
    force_title = False
    if len(query) >= 2 and query[0] == query[-1] and query[0] in ('"', "'", "“"):
        query = query[1:-1].strip("”").strip()
        force_title = True
        if not query:
            return "text", usage

    # Numeric pick from this chat's previous listing.
    note = ""
    if not force_title and re.fullmatch(r"#?\d{1,3}", query):
        n = int(query.lstrip("#"))
        with _last_results_lock:
            keys = _last_results.get(chat_id) or []
        if 1 <= n <= len(keys):
            entry = _entry_by_key(keys[n - 1])
            if entry:
                return "card", (entry.get("subject") or "(no subject)", [entry], 1)
            return "text", f"Result {n} is no longer in the index — /search it again."
        if keys:
            return "text", (
                f"Pick 1–{len(keys)} from your last search, or /search a new title.\n"
                f'To search “{query}” as a title instead, quote it: /search "{query}"')
        note = f"(no previous listing in this chat — searching “{query}” as a title)\n\n"

    exact, results, exact_title, total = _search_entries(query)
    if exact:
        return "card", (exact.get("subject") or "(no subject)", [exact], 1)
    if not results:
        with _store_lock:
            idx_count = len(_load_index().get("emails", []))
        if idx_count == 0:
            msg = (note + "The email index is still empty — the first mailbox scan may "
                   "still be running (the bot just started)")
            if _last_scan_info.get("error"):
                msg += f", and the last scan failed: {_last_scan_info['error']}"
            return "text", msg + ".\nCheck /status, or force a scan with /scan."
        return "text", (
            note + f"No email found for “{query}” in the last {WINDOW_DAYS} days.\n"
            f"Note: only these folders are indexed — {_folders_label()}. "
            "If the email lives elsewhere (e.g. the parent OSE Pending folder or "
            "CLOSED EMAILS), add that folder to TOBOT_IMAP_FOLDERS in .env.\n"
            "Tips: try fewer words from the title, or paste the exact Message-ID.\n"
            "A /scan forces a fresh mailbox re-scan.")
    if exact_title:
        # Exact title match: ONE card with the whole conversation — the
        # original email plus every Re:/Fwd: copy, oldest first.
        with _last_results_lock:
            _last_results[chat_id] = [entry_key(e) for e in results]
        return "card", (results[0].get("subject") or query, results, total)
    if len(results) == 1:
        return "card", (results[0].get("subject") or "(no subject)", results, 1)
    with _last_results_lock:
        _last_results[chat_id] = [entry_key(e) for e in results]
    return "text", note + _format_listing(query, results)


def _search_and_reply(chat_id: str, message_id: str, query: str) -> None:
    kind, payload = handle_search(chat_id, query)
    if kind == "text":
        reply_text(chat_id, message_id, payload)
        return
    title, entries, total = payload
    # Title resolved to Message-ID(s) — now retrieve those emails' contents
    # live. Every entry in the card gets a fetch attempt (bounded by
    # SEARCH_MAX_RESULTS), so "every copy" really shows every copy.
    entries = fetch_contents(entries, limit=len(entries))
    cards = _cards_for_entries(title, entries, total)
    if not reply_card(chat_id, message_id, cards[0]):
        # Card rejected (e.g. missing permission) — plain-text fallback.
        note = f"(+{len(entries) - 1} more copies of this title — card view failed)\n\n" \
            if len(entries) > 1 else ""
        reply_text(chat_id, message_id, note + _format_details(entries[0]))
        return
    for card in cards[1:]:
        if not reply_card(chat_id, message_id, card):
            print("[lark] follow-up card failed — remaining content dropped", flush=True)
            break


# ===================== Command router =====================
HELP_TEXT = (
    "TObot — email search bot 📮\n"
    "──────────\n"
    "/search <email title> — the title finds the email's Message-ID, then the ID\n"
    "  retrieves that exact email's content live from the mailbox\n"
    "/search <exact full title> — ONE card with the whole conversation\n"
    "  (the original email + every Re:/Fwd: reply, oldest first)\n"
    "/search <Message-ID> — direct exact lookup (the accurate key)\n"
    "/search <No.> — open result N from your last search listing\n"
    "@TObot <email title> — same as /search (in P2P just type the title)\n"
    "/scan — force a mailbox re-scan now\n"
    "/status — index size, retention window, last scan\n"
    "/help — this help\n"
    "──────────\n"
    f"Mailbox: {MAIL_USER or '(not set)'} | Folders: {_folders_label()}\n"
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
        f"Folders: {_folders_label()}",
        f"Last scan: {last.get('when') or '(not yet)'} — "
        f"{last.get('scanned', 0)} in window, {last.get('duration_sec', 0)}s",
    ]
    stats = last.get("folders") or {}
    for folder, count in stats.items():
        lines.append(f"  {folder}: {count}")
    if not stats and not SCAN_ALL_FOLDERS:
        for folder in IMAP_FOLDERS:
            lines.append(f"  {folder}: (not scanned)")
    if last.get("running_since"):
        lines.append(f"⏳ Scan in progress since {last['running_since']} — "
                     "the first one backfills the whole window and can take several minutes.")
    if last.get("error"):
        lines.append(f"Last scan error: {last['error']}")
    return "\n".join(lines)


def _do_scan_command(chat_id: str, message_id: str) -> None:
    reply_text(chat_id, message_id, "⏳ Scanning mailbox…")
    try:
        seen = scan_mailbox()
        reply_text(chat_id, message_id,
                   f"✅ Scan done — {seen} emails indexed in the {WINDOW_DAYS}-day window.")
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
        action = lambda: _search_and_reply(chat_id, message_id, t[len("/search"):])
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
            action = lambda: _search_and_reply(chat_id, message_id, t)
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
        f"folders={_folders_label()} window={WINDOW_DAYS}d interval={SCAN_INTERVAL_SEC}s",
        flush=True,
    )
    threading.Thread(target=_scanner_daemon, daemon=True, name="scanner").start()
    run_ws_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
