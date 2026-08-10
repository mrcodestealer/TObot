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
    /machine <name(s)>      machine status card (🟢/🔴 emoji) from
                            webmachine_data.json (kept fresh by the scrape)
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
import hashlib
import html as html_lib
import http
import imaplib
import io
import json
import os
import re
import secrets
import smtplib
import ssl
import subprocess
import sys
import uuid
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from email.header import Header, decode_header, make_header
from email.mime.text import MIMEText
from email.utils import formatdate, getaddresses, make_msgid, parseaddr, parsedate_to_datetime
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
# Outgoing replies (/reply) — same account as IMAP unless overridden.
MAIL_SMTP_HOST = _env("MAIL_SMTP_HOST", "MAINTENANCE_MAIL_SMTP_HOST", default="smtp.larksuite.com")
MAIL_SMTP_PORT = int(_env("MAIL_SMTP_PORT", "MAINTENANCE_MAIL_SMTP_PORT", default="465"))

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

# ===================== /csupdate — AI thread review =====================
# qwen (via the local Ollama OpenAI endpoint) reads a whole email thread —
# images included — and explains the issue, the solution, the current status,
# and whether the thread is stale and needs a follow-up.
CSUPDATE_MODEL = _env("CSUPDATE_MODEL", "BOT_CHAT_MODEL", default="qwen3.6:35b-a3b")
CSUPDATE_API_BASE = _env("CSUPDATE_API_BASE", "BOT_CHAT_API_BASE",
                         default="http://127.0.0.1:11434/v1").rstrip("/")
CSUPDATE_API_KEY = _env("CSUPDATE_API_KEY", "BOT_CHAT_API_KEY", default="ollama")
CSUPDATE_TIMEOUT = max(30, int(_env("CSUPDATE_TIMEOUT", default="600")))
# qwen3.6 on this server needs reasoning_effort=none (see osedutybot /pldtprefix);
# set CSUPDATE_REASONING=off to omit the field entirely.
CSUPDATE_REASONING = _env("CSUPDATE_REASONING", default="none")
CSUPDATE_STALE_DAYS = max(1, int(_env("CSUPDATE_STALE_DAYS", default="2")))
CSUPDATE_MAX_THREADS = max(1, int(_env("CSUPDATE_MAX_THREADS", default="5")))
CSUPDATE_MAX_IMAGES = max(0, int(_env("CSUPDATE_MAX_IMAGES", default="4")))
# /searchwithoutai has no LLM cost — allow many more titles than the AI commands.
NOAI_MAX_TITLES = max(1, int(_env("TOBOT_NOAI_MAX_TITLES", default="20")))
CSUPDATE_CHARS_PER_MAIL = max(500, int(_env("CSUPDATE_CHARS_PER_MAIL", default="4000")))
CSUPDATE_TOTAL_CHARS = max(2000, int(_env("CSUPDATE_TOTAL_CHARS", default="24000")))

# Inline images: extract from the email, upload to Lark, embed in the card.
SHOW_IMAGES = _env("TOBOT_SHOW_IMAGES", default="1").lower() not in ("0", "false", "no", "off")
IMAGES_PER_EMAIL = max(0, int(_env("TOBOT_IMAGES_PER_EMAIL", default="8")))
IMAGE_MIN_BYTES = max(0, int(_env("TOBOT_IMAGE_MIN_BYTES", default="2000")))
IMAGE_MAX_BYTES = max(10000, int(_env("TOBOT_IMAGE_MAX_BYTES", default="9000000")))
CARD_IMAGES_MAX = max(1, int(_env("TOBOT_CARD_IMAGES_MAX", default="24")))
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


# ===================== Image upload (inline email images → Lark) =====================
_img_key_cache: "OrderedDict[str, str]" = OrderedDict()   # sha256(bytes) → Lark image_key
_img_key_lock = threading.Lock()
_IMG_EXT = {"image/png": "png", "image/jpeg": "jpg", "image/jpg": "jpg",
            "image/gif": "gif", "image/webp": "webp", "image/bmp": "bmp"}


def upload_image_bytes(data: bytes, mime: str) -> str:
    """Upload raw image bytes to Lark; returns image_key ('' on failure)."""
    token = get_tenant_access_token()
    if not token:
        return ""
    ext = _IMG_EXT.get((mime or "").lower(), "png")
    try:
        resp = requests.post(
            "https://open.larksuite.com/open-apis/im/v1/images",
            headers={"Authorization": f"Bearer {token}"},
            files={"image": (f"image.{ext}", io.BytesIO(data), mime or "image/png")},
            data={"image_type": "message"},
            timeout=30,
        ).json()
    except Exception as ex:
        print(f"[img] upload failed: {ex!r}", flush=True)
        return ""
    if resp.get("code") == 0:
        return str((resp.get("data") or {}).get("image_key") or "")
    print(f"[img] upload rejected: {resp.get('code')} {resp.get('msg')}", flush=True)
    return ""


def _prepare_image_keys(images: list[tuple[str, bytes]]) -> list[str]:
    """Upload each usable image (deduped by content hash, cached) → image_keys."""
    if not SHOW_IMAGES or not images:
        return []
    keys: list[str] = []
    for mime, data in images:
        if len(keys) >= IMAGES_PER_EMAIL:
            break
        if not data or len(data) < IMAGE_MIN_BYTES or len(data) > IMAGE_MAX_BYTES:
            continue
        h = hashlib.sha256(data).hexdigest()
        with _img_key_lock:
            key = _img_key_cache.get(h)
            if key:
                _img_key_cache.move_to_end(h)
        if not key:
            key = upload_image_bytes(data, mime)
            if key:
                with _img_key_lock:
                    _img_key_cache[h] = key
                    _img_key_cache.move_to_end(h)
                    while len(_img_key_cache) > 1000:
                        _img_key_cache.popitem(last=False)
        if key and key not in keys:
            keys.append(key)
    return keys


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


# Inline-image placeholder tokens senders' clients leave in the plain-text
# alternative ("[image]", "[image: logo.png]", "[cid:xxx]") — noise once the
# real images are shown, so they're removed from the displayed text.
_IMG_PLACEHOLDER_RE = re.compile(r"\[\s*(?:image|cid)\b[^\]]*\]", re.I)
_DATA_URI_RE = re.compile(r"data:image/(png|jpe?g|gif|webp|bmp);base64,([A-Za-z0-9+/=\s]+)", re.I)


# Untrusted email: bound how many image parts we ever decode into memory, and
# skip any single part whose ENCODED size already exceeds the max (so a 50 MB
# inline image is never base64-decoded just to be thrown away by the filter).
_MAX_IMAGE_PARTS_SCANNED = 40

# Where the quoted previous-message history begins inside an HTML reply — used
# to keep only images that belong to the NEW part of the message (a reply
# re-embeds the whole prior thread's inline images, which we must NOT re-show).
_HTML_QUOTE_BOUNDARY = re.compile(
    r"<blockquote\b"
    r'|class=["\']?[^"\']*gmail_quote'
    r'|id=["\']?(?:divRplyFwdMsg|OLK_SRC_BODY_SECTION|mail-editor-reference-message-container|appendonsend)'
    r'|<div[^>]+border-top:[^>]*>\s*(?:<[^>]+>\s*)*(?:from|发件人|寄件者|de)\s*[:：]'
    r"|(?:<b>|<strong>)\s*(?:from|发件人|寄件者)\s*[:：]",
    re.I,
)
_CID_SRC_RE = re.compile(r"""src\s*=\s*["']?\s*cid:([^"'>\s]+)""", re.I)


def _encoded_len(part: email.message.Message) -> int:
    try:
        payload = part.get_payload(decode=False)
        return len(payload) if isinstance(payload, str) else 0
    except Exception:
        return 0


def _cid_key(raw_cid: str) -> str:
    """Normalize a Content-ID / cid reference for matching (drop <>, @domain)."""
    c = (raw_cid or "").strip().strip("<>").strip().lower()
    return c.split("@", 1)[0] if "@" in c else c


def _html_new_region(html: str) -> tuple[str, bool]:
    """(head_html_before_quoted_history, quote_boundary_found)."""
    m = _HTML_QUOTE_BOUNDARY.search(html or "")
    if m:
        return html[:m.start()], True
    return html, False


def extract_email_parts(msg: email.message.Message) -> tuple[str, list[tuple[str, bytes]]]:
    """(body_text, [(mime, bytes), ...]) — text plus the NEW message's images.

    A reply re-embeds every earlier message's inline images; on a reply we keep
    only images referenced in the new (pre-quote) HTML — plus genuine
    attachments — so quoted-history pictures aren't re-shown. An original email
    (no quoted history) keeps all its images. Bounded because email bytes are
    untrusted; the caller applies the final size/count filters.
    """
    plain, html = "", ""
    # Collected image parts as (mime, data, cid_key, is_attachment).
    img_parts: list[tuple[str, bytes, str, bool]] = []
    enc_cap = int(IMAGE_MAX_BYTES * 4 / 3) + 1024  # base64 inflates ~4/3
    parts = list(msg.walk()) if msg.is_multipart() else [msg]
    for part in parts:
        if part.is_multipart():
            continue
        ctype = (part.get_content_type() or "").lower()
        if part.get_content_maintype() == "image":
            if len(img_parts) >= _MAX_IMAGE_PARTS_SCANNED or _encoded_len(part) > enc_cap:
                continue
            try:
                data = part.get_payload(decode=True)
            except Exception:
                data = None
            if data and len(data) <= IMAGE_MAX_BYTES:
                disp = str(part.get("Content-Disposition") or "").lower()
                img_parts.append((ctype, data, _cid_key(part.get("Content-ID") or ""),
                                  "attachment" in disp))
            continue
        disp = str(part.get("Content-Disposition") or "").lower()
        if "attachment" in disp:
            continue
        if ctype == "text/plain" and not plain:
            plain = _decode_part(part)
        elif ctype == "text/html" and not html:
            html = _decode_part(part)
    text = plain.strip() or _html_to_text(html)

    # Is this a reply/forward? The subject prefix (Re:/Fwd:/回复/转发…) is the
    # reliable signal; a detected HTML quote boundary also counts. On a reply we
    # must not re-show the quoted chain's inline images. When we CAN localize the
    # new region (boundary found) we keep images referenced there; when we can't,
    # we drop all inline images and keep only genuine attachments — erring toward
    # hiding quoted pictures rather than showing the wrong ones.
    subj = _decode_hdr(msg.get("Subject"))
    norm_subj = re.sub(r"\s+", " ", subj.casefold().strip())
    head_html, quoted = _html_new_region(html) if html else ("", False)
    is_reply = quoted or (bool(norm_subj) and _base_subject(subj) != norm_subj)
    new_cids = {_cid_key(c) for c in _CID_SRC_RE.findall(head_html)} if (html and quoted) else set()
    images: list[tuple[str, bytes]] = []
    for mime, data, cid, is_att in img_parts:
        if not is_reply:
            keep = True                    # original message — keep all its images
        elif is_att:
            keep = True                    # a file attached to THIS message
        elif quoted and cid and cid in new_cids:
            keep = True                    # inline image referenced in the new part
        else:
            keep = False                   # quoted-history inline image
        if keep:
            images.append((mime, data))

    # Data: URI images: whole HTML for an original, only the new region for a
    # reply with a known boundary, none for a reply we couldn't localize.
    if not is_reply:
        src_html = html
    elif quoted:
        src_html = head_html
    else:
        src_html = ""
    if src_html:
        for m in _DATA_URI_RE.finditer(src_html):
            if len(images) >= _MAX_IMAGE_PARTS_SCANNED:
                break
            b64 = re.sub(r"\s+", "", m.group(2))
            if len(b64) > enc_cap:
                continue
            try:
                data = base64.b64decode(b64)
            except Exception:
                continue
            if len(data) <= IMAGE_MAX_BYTES:
                images.append((f"image/{m.group(1).lower().replace('jpg', 'jpeg')}", data))
    # Drop [image]/[cid:…] placeholder tokens now that real images are shown.
    text = _IMG_PLACEHOLDER_RE.sub("", text)
    text = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", text).strip()
    return text, images


def extract_body_text(msg: email.message.Message) -> str:
    """Prefer text/plain; fall back to text/html stripped to text."""
    return extract_email_parts(msg)[0]


def _addr_list(raw: str) -> list[str]:
    """Addresses from a header value.

    Real-world headers break getaddresses two ways: semicolon separators
    (Outlook/Lark style) parse to nothing, and folded headers with CR+LF
    continuations ("…\\r\\n <a@b>") parse to garbage. Normalize whitespace runs
    to single spaces and semicolons to commas before parsing."""
    flat = re.sub(r"\s+", " ", raw or "").replace(";", ",")
    return [a for _n, a in getaddresses([flat]) if a and "@" in a]


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
# index itself stays headers-only. Value: (body, body_full, image_keys).
_body_cache: "OrderedDict[str, tuple[str, bool, list[str]]]" = OrderedDict()
_body_cache_lock = threading.Lock()


def _body_cache_get(key: str) -> Optional[tuple[str, bool, list[str]]]:
    with _body_cache_lock:
        hit = _body_cache.get(key)
        if hit is not None:
            _body_cache.move_to_end(key)
        return hit


def _body_cache_put(key: str, body: str, full: bool, image_keys: list[str]) -> None:
    with _body_cache_lock:
        _body_cache[key] = (body, full, image_keys)
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


# At most this many folders are searched per entry on the fallback path, so
# one /search can't turn into hundreds of IMAP round-trips.
LIVE_FETCH_FOLDER_CAP = max(2, int(_env("TOBOT_LIVE_FETCH_FOLDERS", default="12")))


def _search_folders_for_live_fetch(mail: imaplib.IMAP4, first: str) -> list[str]:
    """Folders to hunt a moved email in: its recorded folder, then the indexed
    folders, then EVERY other server folder (an email closed out of OSE Pending
    typically lands in CLOSED EMAILS, which may not be indexed)."""
    try:
        server = [
            _imap_utf7_decode(raw)
            for raw in _imap_list_folder_names(mail)
            if _imap_utf7_decode(raw).casefold() not in IMAP_EXCLUDE
        ]
    except ImapStaleConnectionError:
        raise
    except Exception:
        server = []
    ordered = ([first] if first else []) + \
        ([] if SCAN_ALL_FOLDERS else list(IMAP_FOLDERS)) + server
    seen: set[str] = set()
    out: list[str] = []
    for n in ordered:
        if n and n.casefold() not in seen:
            seen.add(n.casefold())
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


def _subject_search_uids(mail: imaplib.IMAP4, subject: str) -> list[bytes]:
    """UID SEARCH by subject (sanitized, ASCII-only — cf. osedutybot's
    _uid_search_subject_variants for this same server). [] when unusable."""
    needle = re.sub(r"\s+", " ", (subject or "")).replace('"', " ").replace("\\", " ").strip()
    needle = needle[:200].strip()
    if not needle:
        return []
    try:
        needle.encode("ascii")
    except UnicodeEncodeError:
        return []   # non-ASCII subject needs charset-tagged SEARCH — skip
    for crit in (f'(HEADER Subject "{needle}")', f'(SUBJECT "{needle}")'):
        uids = _uid_search(mail, crit)
        if uids:
            return uids
    return []


def _entry_matches_message(e: dict[str, Any], msg: email.message.Message) -> bool:
    """Same email? Base subject must match, Date within 10 min, sender same."""
    if _base_subject(_decode_hdr(msg.get("Subject"))) != _base_subject(e.get("subject") or ""):
        return False
    want_ts = float(e.get("date_ts") or 0.0)
    if want_ts > 0:
        try:
            dt = parsedate_to_datetime((msg.get("Date") or "").strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if abs(dt.timestamp() - want_ts) > 600:
                return False
        except Exception:
            return False
    want_from = ((e.get("from") or [""])[0] or "").casefold()
    if want_from:
        got_from = [a.casefold() for a in _addr_list(msg.get("From") or "")]
        if got_from and want_from not in got_from:
            return False
    return True


def _fetch_content_for_entry(
    mail: imaplib.IMAP4, e: dict[str, Any]
) -> Optional[email.message.Message]:
    """Retrieve one email — by stored folder+uid first, then by Message-ID
    search (the accurate key) if the email moved. Either way the fetched
    message's Message-ID must match the entry's before it is returned."""
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
                    return msg
    # Accurate path: find the email by its Message-ID wherever it lives now.
    # HEADER search is substring-based, so every hit is verified against the
    # exact Message-ID before its content is accepted (and cached upstream).
    folders = _search_folders_for_live_fetch(mail, e.get("folder") or "")
    needles = _mid_search_needles(mid)
    if want and needles:
        for folder in folders:
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
                    return msg
    # Last resort — emails WITHOUT a Message-ID (some senders omit it) that
    # moved folders: hunt by subject, then verify subject+date+sender match.
    if e.get("subject"):
        for folder in folders:
            if not _select_folder_resolved(mail, folder):
                continue
            uids = _subject_search_uids(mail, e["subject"])
            for uid_b in list(reversed(uids))[:5]:
                msg = _fetch_uid_body(mail, uid_b.decode(errors="replace"))
                if msg is not None and _entry_matches_message(e, msg):
                    return msg
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
            out[key] = {**e, "body": cached[0], "body_full": cached[1],
                        "image_keys": cached[2]}
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
                    msg = _fetch_content_for_entry(mail, e)
                    key = entry_key(e)
                    if msg is None:
                        # Attempted but not found anywhere — genuinely missing
                        # (distinct from "never attempted": limit/stale abort).
                        out[key] = {**e, "fetch_missing": True}
                        continue
                    text, images = extract_email_parts(msg)
                    image_keys = _prepare_image_keys(images)
                    full = len(text) <= BODY_STORE_MAX_CHARS
                    text = text[:BODY_STORE_MAX_CHARS]
                    _body_cache_put(key, text, full, image_keys)
                    out[key] = {**e, "body": text, "body_full": full,
                                "image_keys": image_keys}
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


def _from_display(entry: dict[str, Any]) -> str:
    """Sender for card markdown — Lark's markdown swallows <addr@host> as a
    tag, so angle brackets become visible ‹›."""
    raw = entry.get("from_raw") or ", ".join(entry.get("from") or []) or "?"
    return raw.replace("<", "‹").replace(">", "›")


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
    shown_imgs: set[str] = set()   # dedupe repeated images (e.g. signature logos) per card
    img_budget = [CARD_IMAGES_MAX]
    for i, e in enumerate(entries, 1):
        if i > 1:
            emit({"tag": "hr"}, 20)
        # Keep the card lean: just who sent it, when, and the content.
        meta = []
        if n > 1:
            meta.append(f"**#{i}**")
        meta.append(f"**From:** {_from_display(e)}")
        meta.append(f"**Date:** {_fmt_date(e)} ({MAIL_TZ})")
        meta_md = "\n".join(meta)
        emit({"tag": "markdown", "content": meta_md}, len(meta_md))
        body = _display_body(e)
        has_imgs = bool(e.get("image_keys"))
        if not body:
            if has_imgs:
                body = ""   # image-only email — the pictures below are the content
            elif "body" in e:
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
        if body:
            parts = _split_body(body, CARD_CHARS_BUDGET)
            for k, piece in enumerate(parts, 1):
                if len(parts) > 1:
                    piece = f"*(content part {k}/{len(parts)})*\n" + piece
                emit({"tag": "markdown", "content": piece}, len(piece))
        # Inline images uploaded for this email — each unique one shown once
        # per card (repeated signature logos collapse to one).
        for img_key in e.get("image_keys") or []:
            if img_budget[0] <= 0:
                break
            if img_key in shown_imgs:
                continue
            shown_imgs.add(img_key)
            img_budget[0] -= 1
            emit({
                "tag": "img",
                "img_key": img_key,
                "alt": {"tag": "plain_text", "content": "email image"},
                "mode": "fit_horizontal",
                "preview": True,
            }, 300)
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


# ===================== /csupdate — AI thread review =====================
_THINK_RE = re.compile(r"<think>.*?</think>", re.S)


def _strip_think(s: str) -> str:
    return _THINK_RE.sub("", s or "").strip()


def _llm_chat(messages: list[dict[str, Any]]) -> str:
    """One chat completion against the local OpenAI-compatible endpoint."""
    payload: dict[str, Any] = {
        "model": CSUPDATE_MODEL,
        "messages": messages,
        "stream": False,
    }
    if CSUPDATE_REASONING and CSUPDATE_REASONING.lower() != "off":
        payload["reasoning_effort"] = CSUPDATE_REASONING
    resp = requests.post(
        f"{CSUPDATE_API_BASE}/chat/completions",
        headers={"Authorization": f"Bearer {CSUPDATE_API_KEY}",
                 "Content-Type": "application/json"},
        json=payload,
        timeout=CSUPDATE_TIMEOUT,
    )
    data = resp.json()
    if resp.status_code != 200 or data.get("error"):
        raise RuntimeError(f"LLM error {resp.status_code}: {str(data)[:300]}")
    content = ((data.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    return _strip_think(content)


def _parse_csupdate_titles(arg: str) -> list[str]:
    out: list[str] = []
    for ln in (arg or "").splitlines():
        t = ln.strip().strip('"').strip()
        if t and t not in out:
            out.append(t)
    return out


def _days_since_ts(ts: float) -> int:
    if ts <= 0:
        return 0
    tz = _local_tz()
    return max(0, (datetime.now(tz).date() - datetime.fromtimestamp(ts, tz).date()).days)


def _resolve_thread(title: str) -> list[dict[str, Any]]:
    """Title (or Message-ID) → the whole thread's entries, oldest first."""
    exact, results, exact_title, _total = _search_entries(title)
    if exact is not None:
        # Message-ID given — expand to its thread via the subject.
        _e2, r2, et2, _t2 = _search_entries(exact.get("subject") or "")
        return r2 if et2 and r2 else [exact]
    if exact_title:
        return results
    if results:
        best = results[0]
        _e2, r2, et2, _t2 = _search_entries(best.get("subject") or "")
        return r2 if et2 and r2 else [best]
    return []


def _build_csupdate_prompt(title: str, mails: list[dict[str, Any]],
                           days_old: int, image_count: int) -> tuple[str, str]:
    """(system_prompt, user_text) for one thread review.

    ``mails``: [{"from":…, "date":…, "text":…}, …] oldest→newest.
    """
    tz = MAIL_TZ
    today = datetime.now(_local_tz()).strftime("%Y-%m-%d")
    last_date = mails[-1]["date"] if mails else "?"
    system = (
        "You are an operations assistant reviewing an internal email thread for the duty team. "
        f"Today is {today} ({tz}). Answer concisely in short sections:\n"
        "1. ISSUE — what this email thread is about (who reported what).\n"
        "2. SOLUTION — the fix/answer/conclusion if any message contains one; otherwise say none was given yet.\n"
        "3. STATUS — current state (resolved / waiting on someone / ongoing) and who the ball is with.\n"
        "4. UPDATE CHECK — the newest message is "
        f"{days_old} day(s) old (sent {last_date}, today {today}). "
        f"If it is {CSUPDATE_STALE_DAYS} or more days old AND the thread is not clearly resolved, "
        "start this section with '⚠️ NEEDS UPDATE' and say a follow-up should be sent; "
        "otherwise say the thread is up to date or resolved.\n"
        "If screenshots/images are attached, use them to understand the issue better and mention "
        "anything important you see in them. Do not invent facts not present in the thread."
    )
    parts = [f'Email thread: "{title}" — {len(mails)} message(s), oldest first.']
    if image_count:
        parts.append(f"({image_count} image(s) from the thread are attached after the text.)")
    budget = CSUPDATE_TOTAL_CHARS
    for i, m in enumerate(mails, 1):
        text = (m.get("text") or "").strip() or "(no text content)"
        if len(text) > CSUPDATE_CHARS_PER_MAIL:
            text = text[:CSUPDATE_CHARS_PER_MAIL].rstrip() + " …(trimmed)"
        block = f"\n[#{i}] From: {m.get('from') or '?'} | Date: {m.get('date') or '?'}\n{text}"
        if budget - len(block) < 0 and i < len(mails):
            parts.append(f"\n…({len(mails) - i + 1} older message(s) omitted for length)")
            break
        parts.append(block)
        budget -= len(block)
    return system, "\n".join(parts)


def _image_data_uri(mime: str, data: bytes) -> str:
    return f"data:{mime or 'image/png'};base64," + base64.b64encode(data).decode()


def _pick_thread_images(images: list[tuple[str, bytes]], cap: int) -> list[tuple[str, bytes]]:
    """Biggest-first (screenshots outrank signature logos), deduped by content,
    tiny images dropped, capped at ``cap``."""
    seen_h: set[str] = set()
    uniq: list[tuple[str, bytes]] = []
    for mime, data in sorted(images, key=lambda t: len(t[1]), reverse=True):
        if len(uniq) >= cap:
            break
        if not data or len(data) < IMAGE_MIN_BYTES:
            continue
        h = hashlib.sha256(data).hexdigest()
        if h in seen_h:
            continue
        seen_h.add(h)
        uniq.append((mime, data))
    return uniq


def _llm_analyze(system: str, user_text: str, images: list[tuple[str, bytes]]) -> str:
    """One qwen call; images sent as data: URIs, with a text-only retry if the
    model rejects them. Raises on empty/failed text-only result."""
    content: Any = user_text
    if images:
        content = [{"type": "text", "text": user_text}] + [
            {"type": "image_url", "image_url": {"url": _image_data_uri(m, d)}}
            for m, d in images
        ]
    try:
        analysis = _llm_chat([{"role": "system", "content": system},
                              {"role": "user", "content": content}])
    except Exception as ex:
        if images:
            print(f"[ai] retrying without images: {ex!r}", flush=True)
            analysis = _llm_chat([{"role": "system", "content": system},
                                  {"role": "user", "content": user_text}])
            analysis += "\n\n_(note: the model could not view the attached images)_"
        else:
            raise
    if not analysis.strip():
        raise RuntimeError("LLM returned an empty answer")
    return analysis.strip()


def _not_found_msg(title: str) -> str:
    return (f"No email found for “{title}” in the last {WINDOW_DAYS} days — "
            "check the title with /search first.")


def _mail_view(mail: Optional[imaplib.IMAP4], e: dict[str, Any]
               ) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    """Fetch one entry's content live → ({from,date,text}, images)."""
    text = ""
    images: list[tuple[str, bytes]] = []
    msg = _fetch_content_for_entry(mail, e) if mail is not None else None
    if msg is not None:
        text, images = extract_email_parts(msg)
    if HIDE_QUOTED_HISTORY:
        text = strip_quoted_history(text) or text
    return {"from": e.get("from_raw") or ", ".join(e.get("from") or []) or "?",
            "date": _fmt_date(e), "text": text}, images


def _csupdate_review_one(mail: Optional[imaplib.IMAP4], title: str) -> tuple[str, str]:
    """(thread_title, analysis_text) — reviews the WHOLE thread."""
    entries = _resolve_thread(title)
    if not entries:
        return title, _not_found_msg(title)
    mails: list[dict[str, Any]] = []
    images: list[tuple[str, bytes]] = []
    for e in entries:
        view, imgs = _mail_view(mail, e)
        mails.append(view)
        images.extend(imgs)
    days_old = _days_since_ts(float(entries[-1].get("date_ts") or 0.0))
    uniq = _pick_thread_images(images, CSUPDATE_MAX_IMAGES)
    system, user_text = _build_csupdate_prompt(
        entries[0].get("subject") or title, mails, days_old, len(uniq))
    return entries[0].get("subject") or title, _llm_analyze(system, user_text, uniq)


def _build_searchwithai_prompt(title: str, view: dict[str, Any],
                               days_old: int, image_count: int) -> tuple[str, str]:
    """(system, user_text) for reviewing ONLY the latest message of a thread."""
    today = datetime.now(_local_tz()).strftime("%Y-%m-%d")
    system = (
        "You are an operations assistant for the duty team. You are shown ONLY the "
        f"LATEST email of a thread. Today is {today} ({MAIL_TZ}). Answer concisely:\n"
        "1. SUMMARY — what this latest message says (the key point / request / answer).\n"
        "2. STATUS — from this message, is the matter resolved, waiting on someone, or ongoing?\n"
        f"3. FRESHNESS — this message is {days_old} day(s) old (sent {view.get('date')}). "
        f"If {CSUPDATE_STALE_DAYS}+ days old and not resolved, start with '⚠️ NEEDS UPDATE'.\n"
        "If images are attached, use them and mention what you see. "
        "Do not invent facts not present in the message."
    )
    text = (view.get("text") or "").strip() or "(no text content)"
    if len(text) > CSUPDATE_CHARS_PER_MAIL:
        text = text[:CSUPDATE_CHARS_PER_MAIL].rstrip() + " …(trimmed)"
    parts = [f'Latest email in thread: "{title}"',
             f"From: {view.get('from') or '?'} | Date: {view.get('date') or '?'}"]
    if image_count:
        parts.append(f"({image_count} image(s) attached after the text.)")
    parts.append("\n" + text)
    return system, "\n".join(parts)


def _searchwithai_review_one(mail: Optional[imaplib.IMAP4], title: str) -> tuple[str, str]:
    """(thread_title, analysis_text) — reviews ONLY the newest message."""
    entries = _resolve_thread(title)
    if not entries:
        return title, _not_found_msg(title)
    latest = entries[-1]
    view, images = _mail_view(mail, latest)
    days_old = _days_since_ts(float(latest.get("date_ts") or 0.0))
    uniq = _pick_thread_images(images, CSUPDATE_MAX_IMAGES)
    system, user_text = _build_searchwithai_prompt(
        latest.get("subject") or title, view, days_old, len(uniq))
    shown = entries[0].get("subject") or title
    return shown, _llm_analyze(system, user_text, uniq)


def _ai_card(title: str, analysis: str, subtitle: str) -> dict[str, Any]:
    elements = [{"tag": "markdown", "content": piece}
                for piece in _split_body(analysis, CARD_CHARS_BUDGET)]
    elements.append({"tag": "markdown", "content": subtitle})
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"🤖 {title}"[:150]},
            "template": "purple",
        },
        "elements": elements,
    }


def _csupdate_card(title: str, analysis: str) -> dict[str, Any]:
    return _ai_card(title, analysis,
                    f"*AI review by {CSUPDATE_MODEL} — verify before replying to the thread.*")


def _run_ai_command(chat_id: str, message_id: str, arg: str, *,
                    label: str, usage: str, reviewer, subtitle: str) -> None:
    """Shared multi-title AI driver for /csupdate and /searchwithai.

    ``reviewer(mail, title) -> (shown_title, analysis)``; one card per title,
    processed sequentially, reconnecting IMAP once on a stale connection.
    """
    titles = _parse_csupdate_titles(arg)
    if not titles:
        reply_text(chat_id, message_id, usage)
        return
    dropped = 0
    if len(titles) > CSUPDATE_MAX_THREADS:
        dropped = len(titles) - CSUPDATE_MAX_THREADS
        titles = titles[:CSUPDATE_MAX_THREADS]
    note = f" (first {CSUPDATE_MAX_THREADS} only — {dropped} skipped)" if dropped else ""
    reply_text(chat_id, message_id,
               f"🤖 Reviewing {len(titles)} email(s) with {CSUPDATE_MODEL}{note} — "
               "this can take a few minutes each…")
    try:
        mail = _connect_imap()
    except Exception as ex:
        print(f"[{label}] IMAP connect failed: {ex!r}", flush=True)
        mail = None
    try:
        for title in titles:
            try:
                shown_title, analysis = reviewer(mail, title)
            except ImapStaleConnectionError as ex:
                print(f"[{label}] {ex} — reconnecting", flush=True)
                try:
                    mail = _connect_imap()
                    shown_title, analysis = reviewer(mail, title)
                except Exception as ex2:
                    reply_text(chat_id, message_id, f"❌ “{title}”: {ex2!r}")
                    continue
            except Exception as ex:
                reply_text(chat_id, message_id, f"❌ “{title}”: {ex!r}")
                continue
            if not reply_card(chat_id, message_id, _ai_card(shown_title, analysis, subtitle)):
                reply_text(chat_id, message_id, f"🤖 {shown_title}\n──────────\n{analysis}")
    finally:
        if mail is not None:
            try:
                mail.logout()
            except Exception:
                pass


def _do_csupdate(chat_id: str, message_id: str, arg: str) -> None:
    _run_ai_command(
        chat_id, message_id, arg, label="csupdate",
        usage=("Usage:\n/csupdate\n<email title 1>\n<email title 2>\n…\n"
               "Each title gets a full-thread AI review: issue, solution, status, "
               "and whether the thread needs a follow-up."),
        reviewer=_csupdate_review_one,
        subtitle=f"*AI review by {CSUPDATE_MODEL} — verify before replying to the thread.*",
    )


def _do_searchwithai(chat_id: str, message_id: str, arg: str) -> None:
    _run_ai_command(
        chat_id, message_id, arg, label="searchwithai",
        usage=("Usage:\n/searchwithai\n<email title 1>\n<email title 2>\n…\n"
               "Each title gets an AI review of the LATEST message only: summary, "
               "status, and whether it needs an update."),
        reviewer=_searchwithai_review_one,
        subtitle=f"*AI summary of the latest email by {CSUPDATE_MODEL} — verify before acting.*",
    )


def _do_searchwithoutai(chat_id: str, message_id: str, arg: str) -> None:
    """Show ONLY the latest message of each thread — content + images, no AI."""
    titles = _parse_csupdate_titles(arg)
    if not titles:
        reply_text(chat_id, message_id,
                   "Usage:\n/searchwithoutai\n<email title 1>\n<email title 2>\n…\n"
                   "Shows the LATEST message of each thread (sender + content + images), "
                   "no AI. Use /search for the whole conversation.")
        return
    if len(titles) > NOAI_MAX_TITLES:
        reply_text(chat_id, message_id,
                   f"ℹ️ Showing the first {NOAI_MAX_TITLES} titles — "
                   f"{len(titles) - NOAI_MAX_TITLES} skipped "
                   "(raise TOBOT_NOAI_MAX_TITLES in .env for more).")
        titles = titles[:NOAI_MAX_TITLES]
    for title in titles:
        entries = _resolve_thread(title)
        if not entries:
            reply_text(chat_id, message_id, _not_found_msg(title))
            continue
        latest = entries[-1]
        try:
            withbody = fetch_contents([latest], limit=1)[0]
        except Exception as ex:
            reply_text(chat_id, message_id, f"❌ “{title}”: {ex!r}")
            continue
        cards = _cards_for_entries(latest.get("subject") or title, [withbody], None)
        if len(entries) > 1:
            cards[0]["elements"].insert(0, {
                "tag": "markdown",
                "content": f"*📩 Latest message — newest of {len(entries)} in this thread*",
            })
        ok_sent = True
        for c in cards:
            if not reply_card(chat_id, message_id, c):
                ok_sent = False
                break
        if not ok_sent:
            reply_text(chat_id, message_id, _format_details(withbody))


# ===================== /reply — reply-all with card form =====================
# Flow: "/reply\n<title>\n<title>" → every title must resolve; ONE preview card
# lists each thread's own To/Cc (reply-all to its LATEST message) with an input
# box + Send button. Submitting sends the SAME content to every thread — each
# with its own recipients/threading. Nothing is sent until the button is pressed.
_REPLY_GREETING = "Hi team,"
_REPLY_CLOSING = "Thank you and best regards,"
_REPLY_BATCH_TTL_SEC = 24 * 3600
_REPLY_BATCH_MAX = 20
# Quote the thread's latest message below the reply (like a mail client's reply-all;
# recipients' clients collapse it into the usual show/hide section). 0 disables quoting.
REPLY_QUOTE_CHARS = max(0, int(_env("TOBOT_REPLY_QUOTE_CHARS", default="20000")))

# Recipients known to bounce with "invalid recipient address" — removed from every
# reply-all (with a ⚠️ notice on the preview card). Extend/override with
# TOBOT_REPLY_BLOCKED_RECIPIENTS (comma-separated addresses, or `@domain` to block a domain).
_REPLY_BLOCKED_DEFAULT = (
    "chabelita.honrada@hotelstotsenberg.com,"
    "operationsupport.team@hotelstotsenberg.com,"
    "cxrteam@igo.email"
)
_REPLY_BLOCKED_RECIPIENTS = {
    x.strip().casefold()
    for x in _env("TOBOT_REPLY_BLOCKED_RECIPIENTS", default=_REPLY_BLOCKED_DEFAULT).split(",")
    if x.strip()
}


def _reply_recipient_blocked(addr: str) -> bool:
    al = (addr or "").strip().casefold()
    if not al:
        return False
    if al in _REPLY_BLOCKED_RECIPIENTS:
        return True
    dom = al.rsplit("@", 1)[-1] if "@" in al else ""
    return bool(dom) and f"@{dom}" in _REPLY_BLOCKED_RECIPIENTS

_pending_replies: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_pending_replies_lock = threading.Lock()


# ---- Lark Mail quote block ----------------------------------------------
# Structure ported from osedutybot's proven maintenance auto-reply
# (maintenance_mail.py `_build_lark_quote_html` / `build_reply_message_html`,
# lines 865-1169), which mirrors Lark's own composer output (larksuite/cli
# mail_quote.go). Lark Mail collapses THIS markup into "Show email thread";
# a <blockquote> does not. The reply uses the ``--collapsed`` block (folded,
# like a manual Reply All); the previous message's ORIGINAL HTML is embedded
# verbatim so its own nested history travels along.
_REPLY_SEP = "---------- Original message ----------"
_LARK_QUOTE_WRAPPER = "history-quote-wrapper"
_LARK_REPLY_BLOCK = "adit-html-block--collapsed"
_LARK_QUOTE_BORDER = "border-left: none; padding-left: 0px;"
_LARK_META_STYLE = (
    "padding: 12px; background: rgb(245, 246, 247); color: rgb(31, 35, 41); "
    "border-radius: 4px; margin-bottom: 12px;"
)
_LARK_META_MARGIN = "margin-top: 2px;"
_LARK_SEP_STYLE = "color: rgb(100, 106, 115); margin-top: 24px; margin-bottom: 8px;"
_LARK_ADDR_STYLE = (
    "overflow-wrap: break-word; color: inherit; text-decoration: none; "
    "white-space: pre-wrap; hyphens: none; word-break: break-word; cursor: pointer;"
)


def _lark_esc(s: str) -> str:
    return html_lib.escape(s or "", quote=False)


def _lark_attr(s: str) -> str:
    """Escape for use INSIDE an attribute value (quotes too)."""
    return html_lib.escape(s or "", quote=True)


def _gen_lark_id(prefix: str) -> str:
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return prefix + "".join(secrets.choice(chars) for _ in range(6))


def _quote_labels(subject: str) -> dict[str, str]:
    """Chinese labels when the subject carries CJK, else English (as Lark does)."""
    for ch in subject or "":
        if "一" <= ch <= "鿿":
            return {"from": "发件人", "date": "时间", "subject": "主题",
                    "to": "收件人", "cc": "抄送", "sep": "--------- 原始邮件 ---------"}
    return {"from": "From", "date": "Date", "subject": "Subject",
            "to": "To", "cc": "Cc", "sep": _REPLY_SEP}


def _address_html(from_hdr: str) -> str:
    name, addr = parseaddr(from_hdr or "")
    if addr:
        e, a = _lark_esc(addr), _lark_attr(addr)   # attribute values need quote escaping
        anchor = (f'<a class="quote-head-meta-mailto" data-mailto="mailto:{a}" '
                  f'href="mailto:{a}" style="{_LARK_ADDR_STYLE}">{e}</a>')
    else:
        anchor = _lark_esc(from_hdr)
    if name and addr:
        return f'"{_lark_esc(name)}"&lt;{anchor}&gt;'
    if addr:
        return f"&lt;{anchor}&gt;"
    return anchor


def _meta_row(label: str, content: str) -> str:
    return (f'<div class="lme-line-signal"><span style="">{_lark_esc(label)}: '
            f"{content}</span></div>")


def _body_is_html(s: str) -> bool:
    return bool(re.search(
        r"(?i)<(?:!doctype\s+html|!--|html|head|body|div|p|br|span|table|blockquote)", s or ""))


# Active content and document tags that must not survive into OUR outgoing mail
# (the quoted HTML comes from a third party and is spliced into our document).
_UNSAFE_BLOCK_RE = re.compile(r"(?is)<(script|iframe|object|embed|noscript)\b[^>]*>.*?</\1\s*>")
_UNSAFE_OPEN_RE = re.compile(r"(?is)</?(?:script|iframe|object|embed|noscript|meta|base|link)\b[^>]*>")
_STRAY_DOC_RE = re.compile(r"(?is)</?(?:html|head|body)\b[^>]*>")
_ON_ATTR_RE = re.compile(r"(?is)\son[a-z]+\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s>]+)")
# cid: images point at parts we do NOT carry along — drop them instead of
# shipping broken-image icons in the quoted history.
_CID_IMG_RE = re.compile(r"(?is)<img\b[^>]*\bsrc\s*=\s*[\"']?\s*cid:[^>]*>")


def _balance_divs(s: str) -> str:
    """Make <div> open/close counts match so a malformed quote can't close OUR
    wrapper early (which would spill the history out of the collapsible block)."""
    opens = len(re.findall(r"(?i)<div\b", s))
    closes = len(re.findall(r"(?i)</div\s*>", s))
    if closes > opens:
        for m in reversed(list(re.finditer(r"(?i)</div\s*>", s))[-(closes - opens):]):
            s = s[:m.start()] + s[m.end():]
    elif opens > closes:
        s += "</div>" * (opens - closes)
    return s


def _sanitize_embedded_html(html: str) -> str:
    """Drop outer document wrappers so nested HTML doesn't break Lark quote detection.

    Beyond osedutybot's wrapper stripping this also removes active content and
    balances <div>s — the quoted mail is third-party HTML spliced into our own
    document, and an unbalanced </div> would close the collapsible block early.
    """
    t = (html or "").strip()
    if not t:
        return ""
    t = re.sub(r"(?is)<!DOCTYPE[^>]*>", "", t)
    t = re.sub(r"(?is)<head\b[^>]*>.*?</head>", "", t)
    m = re.search(r"(?is)<body\b[^>]*>(.*)</body>", t)
    if m:
        t = m.group(1)
    t = _UNSAFE_BLOCK_RE.sub("", t)
    t = _UNSAFE_OPEN_RE.sub("", t)
    t = _STRAY_DOC_RE.sub("", t)          # leftovers from malformed documents
    t = _ON_ATTR_RE.sub("", t)            # onclick=… and friends
    t = _CID_IMG_RE.sub("", t)            # images whose parts we don't attach
    return _balance_divs(t.strip()).strip()


def _first_html_part(part: email.message.Message) -> Optional[email.message.Message]:
    """First real text/html body part, WITHOUT descending into attached mails
    (an attached .eml carries its own text/html that is not this message's body)."""
    ctype = (part.get_content_type() or "").lower()
    if ctype == "message/rfc822":
        return None
    if part.is_multipart():
        payload = part.get_payload()
        for sub in payload if isinstance(payload, list) else []:
            if isinstance(sub, email.message.Message):
                got = _first_html_part(sub)
                if got is not None:
                    return got
        return None
    if ctype != "text/html":
        return None
    if "attachment" in str(part.get("Content-Disposition") or "").lower():
        return None
    return part


def extract_body_html_raw(msg: email.message.Message) -> Optional[str]:
    """The message's OWN text/html body, NOT converted to text."""
    part = _first_html_part(msg)
    if part is None:
        # Single-part text/html marked as an attachment is still the body
        # (matches osedutybot, which does not check the disposition there).
        if msg.is_multipart() or (msg.get_content_type() or "").lower() != "text/html":
            return None
        part = msg
    try:
        payload = part.get_payload(decode=True)
    except Exception:
        return None
    if not payload:
        return None
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace").strip() or None
    except (LookupError, UnicodeDecodeError):
        return payload.decode("utf-8", errors="replace").strip() or None


def _build_lark_reply_quote_html(*, from_hdr: str, date_line: str, subject: str,
                                 to_hdr: str, cc_hdr: str, body_html: str) -> str:
    """The collapsible quote block itself (Lark renders it as Show/Hide email thread)."""
    labels = _quote_labels(subject)
    rows = [_meta_row(labels["from"], _address_html(from_hdr))]
    if date_line:
        rows.append(_meta_row(labels["date"], _lark_esc(date_line)))
    if subject:
        rows.append(_meta_row(labels["subject"], _lark_esc(subject)))
    rows.append(_meta_row(labels["to"], _lark_esc(to_hdr)))
    if cc_hdr:
        rows.append(_meta_row(labels["cc"], _lark_esc(cc_hdr)))
    meta_html = (
        f'<div id="{_gen_lark_id("lark-mail-meta-cli")}" class="adit-html-block__header '
        'history-quote-meta-after-forward-title history-quote-meta-wrapper" '
        f'style="{_LARK_META_MARGIN} {_LARK_META_STYLE}">'
        f'<div style="word-break: break-word;">{"".join(rows)}</div></div>'
    )
    sep_html = ('<div class="history-quote-forward-title lme-line-signal history-quote-gap-tag" '
                f'style="{_LARK_SEP_STYLE}">{_lark_esc(labels["sep"])}</div>')
    body_part = f"<div>{body_html}</div>" if body_html else ""
    return (
        f'<div id="{_gen_lark_id("lark-mail-quote-cli")}" class="{_LARK_QUOTE_WRAPPER}">'
        '<div data-html-block="quote" data-mail-html-ignore="">'
        f'<div class="adit-html-block {_LARK_REPLY_BLOCK}" style="{_LARK_QUOTE_BORDER}">'
        f'<div id="{_gen_lark_id("lark-mail-quote-cli")}">{sep_html}{meta_html}{body_part}</div>'
        "</div></div></div>"
    )


def _build_reply_html(text: str, quote_html: str) -> str:
    """Full reply document: our text on top, the collapsible quote below."""
    body_html = "<br>".join(
        _lark_esc(line) for line in (text or "").replace("\r\n", "\n").split("\n"))
    top = ('<div style="word-break:break-word;line-height:1.6;'
           f'font-size:14px;color:rgb(0,0,0);">{body_html}</div>')
    gap = ('<div style="word-break:break-word;line-height:1.6;'
           'font-size:14px;color:rgb(0,0,0);"><br></div>')
    return ("<!DOCTYPE html><html><head>"
            '<meta http-equiv="Content-Type" content="text/html; charset=utf-8">'
            "</head><body>"
            f'<div dir="ltr">{top}{gap}{quote_html}</div>'
            "</body></html>")


def _own_addresses() -> set[str]:
    return {a.strip().casefold() for a in (MAIL_USER,) if a.strip()}


def _reply_subject(subject: str) -> str:
    s = (subject or "").strip()
    return s if re.match(r"^\s*re\s*[::]", s, re.I) else f"Re: {s}"


def _compute_reply_spec(mail: Optional[imaplib.IMAP4],
                        entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Reply-all spec for one thread.

    To = the LATEST message's sender + its To recipients. Cc = EVERYONE who
    appeared anywhere in the thread (From/To/Cc of every message) — so a
    participant isn't dropped just because the newest reply went person-to-
    person without the usual Cc list. Own address removed, order kept, deduped.
    Uses the live latest message when fetchable (accurate headers + References
    for threading), falling back to the indexed headers.
    """
    latest = entries[-1]
    frm = list(latest.get("from") or [])
    to = list(latest.get("to") or [])
    cc = list(latest.get("cc") or [])
    references = ""
    msg = _fetch_content_for_entry(mail, latest) if mail is not None else None
    if msg is not None:
        frm = _addr_list(msg.get("From") or "") or frm
        to = _addr_list(msg.get("To") or "") or to
        cc = _addr_list(msg.get("Cc") or "")
        references = re.sub(r"\s+", " ", msg.get("References") or "").strip()
    own = _own_addresses()
    to_out: list[str] = []
    removed: list[str] = []
    seen: set[str] = set()
    for a in frm + to:
        al = a.strip().casefold()
        if al and al not in own and al not in seen:
            seen.add(al)
            if _reply_recipient_blocked(a):
                removed.append(a.strip())
            else:
                to_out.append(a.strip())
    # Cc: the latest message's Cc first, then every other participant seen in
    # the whole thread (oldest→newest), so the full distribution stays intact.
    thread_addrs: list[str] = list(cc)
    # The ORIGINAL email usually carries the canonical distribution list; fetch
    # it live (it may sit in an unindexed folder, and older index entries may
    # predate the semicolon-Cc parsing fix).
    if len(entries) > 1 and mail is not None:
        orig_msg = _fetch_content_for_entry(mail, entries[0])
        if orig_msg is not None:
            for hdr in ("From", "To", "Cc"):
                thread_addrs.extend(_addr_list(orig_msg.get(hdr) or ""))
    for e in entries:
        thread_addrs.extend(e.get("from") or [])
        thread_addrs.extend(e.get("to") or [])
        thread_addrs.extend(e.get("cc") or [])
    cc_out: list[str] = []
    for a in thread_addrs:
        al = a.strip().casefold()
        if al and al not in own and al not in seen:
            seen.add(al)
            if _reply_recipient_blocked(a):
                removed.append(a.strip())
            else:
                cc_out.append(a.strip())
    # Quoted history below the reply, in Lark Mail's own reply style: a
    # From/Date/Subject/To/Cc header block + the latest message's body as-is
    # (its own nested history travels along), which mail clients collapse into
    # the "Show email thread" section.
    quote_body = ""
    if msg is not None:
        try:
            quote_body = extract_body_text(msg)
        except Exception:
            quote_body = ""
    if not quote_body:
        quote_body = str(latest.get("body") or "")
    quote_body = quote_body.strip()
    # Flatten '>'-style quote markers some clients/servers add when nesting
    # replies (broken further by re-wrapping) — the history then reads like
    # Lark's flat thread style: repeated From/Date/Subject blocks, no '>'.
    quote_body = re.sub(r"(?m)^[ \t]*(?:>[ \t]?)+", "", quote_body)
    if REPLY_QUOTE_CHARS and len(quote_body) > REPLY_QUOTE_CHARS:
        quote_body = quote_body[:REPLY_QUOTE_CHARS] + "\n[... quoted history trimmed ...]"

    def _unfold(s: str) -> str:
        """RFC-folded headers keep their newlines through decoding — collapse
        them so To/Cc lists don't break in the middle of an address."""
        return re.sub(r"\s+", " ", (s or "").strip())

    q_from = _unfold((_decode_hdr(msg.get("From")) if msg is not None else "") or
                     latest.get("from_raw") or ", ".join(frm))
    q_to = _unfold((_decode_hdr(msg.get("To")) if msg is not None else "") or ", ".join(to))
    q_cc = _unfold((_decode_hdr(msg.get("Cc")) if msg is not None else "") or ", ".join(cc))
    q_subject = _unfold((_decode_hdr(msg.get("Subject")) if msg is not None else "") or
                        latest.get("subject") or "")
    q_ts = float(latest.get("date_ts") or 0.0)
    if q_ts > 0:
        q_date = datetime.fromtimestamp(q_ts, _local_tz()).strftime("%a, %b %d, %Y, %H:%M")
    else:
        # Empty when unknown — the quote builder then omits the Date row entirely
        # (osedutybot does the same); only the plain-text header shows a "?".
        q_date = (msg.get("Date") or "").strip() if msg is not None else ""
    quote_header_lines = [
        f"From: {q_from}".strip(),
        f"Date: {q_date or '?'}",
        f"Subject: {q_subject}".rstrip(),
        f"To: {q_to}".rstrip(),
    ]
    if q_cc.strip():
        quote_header_lines.append(f"Cc: {q_cc}")
    # The collapsible Lark quote, built once here (the fetched message is not
    # kept around until the user presses Send). The previous mail's ORIGINAL
    # HTML is embedded so its own history/formatting survives; plain-text-only
    # mails fall back to <pre>.
    quote_html = ""
    if REPLY_QUOTE_CHARS:
        raw_html = extract_body_html_raw(msg) if msg is not None else None
        inner = _sanitize_embedded_html(raw_html) if raw_html and _body_is_html(raw_html) else ""
        if len(inner) > REPLY_QUOTE_CHARS * 10:   # runaway thread → text fallback
            inner = ""
        if not inner and quote_body:
            inner = f'<pre style="white-space:pre-wrap">{_lark_esc(quote_body)}</pre>'
        # Built even when the body came out empty (attachment-only mail): the
        # header block alone still gives the collapsible thread, as upstream does.
        quote_html = _build_lark_reply_quote_html(
            from_hdr=q_from, date_line=q_date, subject=q_subject,
            to_hdr=q_to, cc_hdr=q_cc, body_html=inner,
        )
    mid = (latest.get("message_id") or "").strip()
    return {
        "title": entries[0].get("subject") or latest.get("subject") or "",
        "subject": _reply_subject(latest.get("subject") or ""),
        "to": to_out,
        "cc": cc_out,
        "removed": removed,
        "quote_body": quote_body,
        "quote_header": "\n".join(quote_header_lines),
        "quote_html": quote_html,
        "in_reply_to": mid,
        "references": (f"{references} {mid}".strip() if mid else references),
        "latest_from": latest.get("from_raw") or ", ".join(frm) or "?",
        "latest_date": _fmt_date(latest),
    }


def _reply_batch_new(chat_id: str, specs: list[dict[str, Any]]) -> str:
    batch_id = uuid.uuid4().hex[:12]
    with _pending_replies_lock:
        now = time.time()
        for k in list(_pending_replies):
            if now - _pending_replies[k]["created"] > _REPLY_BATCH_TTL_SEC:
                del _pending_replies[k]
        while len(_pending_replies) >= _REPLY_BATCH_MAX:
            _pending_replies.popitem(last=False)
        _pending_replies[batch_id] = {
            "chat_id": chat_id, "specs": specs, "created": now, "state": "pending",
        }
    return batch_id


def _reply_batch_claim(batch_id: str, new_state: str = "sending") -> Optional[dict[str, Any]]:
    """Atomically move a batch pending→new_state; None if unknown/already used."""
    with _pending_replies_lock:
        b = _pending_replies.get(batch_id)
        if b is None or b["state"] != "pending":
            return None
        b["state"] = new_state
        return b


def _esc_addrs(addrs: list[str]) -> str:
    return ", ".join(addrs).replace("<", "‹").replace(">", "›") or "(none)"


def _md(content: str) -> dict[str, Any]:
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _reply_preview_card(batch_id: str, specs: list[dict[str, Any]]) -> dict[str, Any]:
    """Card schema 2.0 — same structure as osedutybot's proven form cards."""
    n = len(specs)
    elements: list[dict[str, Any]] = [_md(
        f"✅ **All {n} email(s) found.** The same content will be sent to "
        "each thread — every reply uses ITS OWN recipients "
        "(reply-all to that thread's latest message):")]
    for i, s in enumerate(specs, 1):
        elements.append({"tag": "hr"})
        lines = [
            f"**#{i} {s['title']}**",
            f"**To:** {_esc_addrs(s['to'])}",
            f"**Cc:** {_esc_addrs(s['cc'])}",
        ]
        if s.get("removed"):
            lines.append("⚠️ **Detected invalid recipient address — removed:** "
                         f"{_esc_addrs(s['removed'])}")
        lines.append(f"*(replying to the latest message — from {s['latest_from'].replace('<', '‹').replace('>', '›')}, {s['latest_date']})*")
        elements.append(_md("\n".join(lines)))
    elements.append({"tag": "hr"})
    elements.append(_md("**Edit the reply below — it is sent exactly as shown:**\n"
                        "*(each thread's latest message is quoted below your reply, "
                        "like a normal reply-all — recipients can show/hide it)*"))
    elements.append({
        "tag": "form",
        "name": "reply_form",
        "elements": [
            {
                "tag": "input",
                "name": "reply_content",
                "input_type": "multiline_text",
                "rows": 8,
                "auto_resize": True,
                "width": "fill",
                "label": {"tag": "plain_text", "content": "Content"},
                "label_position": "top",
                # Template pre-filled in the box; the user edits it in place
                # (blank lines below the closing too, for the sign-off).
                "default_value": f"{_REPLY_GREETING}\n\n\n\n{_REPLY_CLOSING}\n\n",
                "placeholder": {"tag": "plain_text",
                                "content": "Write the reply content…"},
                "required": True,
                # Lark rejects anything above its default maximum of 1000
                # (default_value must stay within it too — cf. osedutybot).
                "max_length": 1000,
            },
            {
                "tag": "button",
                "name": "send_reply",
                "text": {"tag": "plain_text", "content": f"📤 Send reply to all {n} email(s)"},
                "type": "primary",
                "form_action_type": "submit",
                "behaviors": [
                    {"type": "callback", "value": {"batch": batch_id, "action": "send"}},
                ],
            },
        ],
    })
    elements.append({
        "tag": "button",
        "name": "cancel_reply",
        "text": {"tag": "plain_text", "content": "✖️ Cancel — send nothing"},
        "type": "danger",
        "behaviors": [
            {"type": "callback", "value": {"batch": batch_id, "action": "cancel"}},
        ],
    })
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "template": "green",
            "title": {"tag": "plain_text",
                      "content": f"✉️ Reply All — {n} email(s) ready"[:150]},
        },
        "body": {"elements": elements},
    }


def _do_reply(chat_id: str, message_id: str, arg: str) -> None:
    titles = _parse_csupdate_titles(arg)
    if not titles:
        reply_text(chat_id, message_id,
                   "Usage:\n/reply\n<email title 1>\n<email title 2>\n…\n"
                   "Finds every email, shows each one's reply-all To/Cc in a card, "
                   "and lets you fill in ONE content that is sent to all of them.")
        return
    try:
        mail = _connect_imap()
    except Exception as ex:
        print(f"[reply] IMAP connect failed: {ex!r}", flush=True)
        mail = None
    specs: list[dict[str, Any]] = []
    missing: list[str] = []
    no_recipients: list[str] = []
    try:
        for title in titles:
            entries = _resolve_thread(title)
            if not entries:
                missing.append(title)
                continue
            try:
                spec = _compute_reply_spec(mail, entries)
            except ImapStaleConnectionError:
                try:
                    mail = _connect_imap()
                except Exception:
                    mail = None
                spec = _compute_reply_spec(mail, entries)
            if not spec["to"]:
                no_recipients.append(title)
                continue
            specs.append(spec)
    finally:
        if mail is not None:
            try:
                mail.logout()
            except Exception:
                pass
    if missing or no_recipients:
        lines = ["❌ Not every email was found — nothing prepared, nothing will be sent."]
        for t in titles:
            if t in missing:
                lines.append(f"  ❌ {t} — not found (check with /search)")
            elif t in no_recipients:
                lines.append(f"  ⚠️ {t} — found, but no reply recipients besides {MAIL_USER}")
            else:
                lines.append(f"  ✅ {t}")
        reply_text(chat_id, message_id, "\n".join(lines))
        return
    batch_id = _reply_batch_new(chat_id, specs)
    if not reply_card(chat_id, message_id, _reply_preview_card(batch_id, specs)):
        reply_text(chat_id, message_id,
                   "❌ Couldn't render the reply card (check bot card permissions) — "
                   "nothing was sent.")


def send_reply_email(spec: dict[str, Any], content: str) -> None:
    """One SMTP reply-all send. Raises on failure.

    With a quote: a SINGLE ``text/html`` part carrying Lark's own quote markup,
    so Lark Mail folds the history into **Show/Hide email thread** exactly like
    a manual Reply All (osedutybot's proven approach — adding a plain-text
    alternative makes Lark expand the quote as raw text instead). Without a
    quote it stays a plain-text mail.
    """
    content = (content or "").strip()
    quote_html = (spec.get("quote_html") or "") if REPLY_QUOTE_CHARS else ""
    if quote_html:
        msg: email.message.Message = MIMEText(
            _build_reply_html(content, quote_html), "html", "utf-8")
        msg.replace_header("Content-Type", 'text/html; charset="utf-8"')
    else:
        quote = (spec.get("quote_body") or "").strip()
        header = (spec.get("quote_header") or "").strip()
        body = content
        if REPLY_QUOTE_CHARS and quote:
            body = f"{body}\n\n\n{header}\n\n{quote}" if header else f"{body}\n\n\n{quote}"
        msg = MIMEText(body + "\n", "plain", "utf-8")
    msg["Subject"] = Header(spec["subject"], "utf-8")
    msg["From"] = MAIL_USER
    msg["To"] = ", ".join(spec["to"])
    if spec["cc"]:
        msg["Cc"] = ", ".join(spec["cc"])
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    if spec.get("in_reply_to"):
        msg["In-Reply-To"] = spec["in_reply_to"]
    if spec.get("references"):
        msg["References"] = spec["references"]
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(MAIL_SMTP_HOST, MAIL_SMTP_PORT, context=ctx, timeout=60) as smtp:
        smtp.login(MAIL_USER, MAIL_PASSWORD)
        smtp.send_message(msg)


def _send_reply_batch(batch: dict[str, Any], content: str) -> None:
    """Background worker: send every reply, then report results to the chat."""
    results: list[str] = []
    ok_count = 0
    for s in batch["specs"]:
        try:
            send_reply_email(s, content)
            ok_count += 1
            results.append(f"  ✅ {s['title']}")
        except Exception as ex:
            print(f"[reply] send failed for {s['title']!r}: {ex!r}", flush=True)
            results.append(f"  ❌ {s['title']} — {ex!r}")
    batch["state"] = "sent"
    # Quoted HTML can be ~100 KB per thread — drop it once sent (batches live 24h).
    for s in batch["specs"]:
        s["quote_html"] = ""
        s["quote_body"] = ""
    summary = (f"📤 Reply sent to {ok_count}/{len(batch['specs'])} email(s) "
               f"from {MAIL_USER}:\n" + "\n".join(results))
    reply_text(batch["chat_id"], "", summary)


def _reply_done_card(text: str, template: str) -> dict[str, Any]:
    """Replacement card (schema 2.0) shown after Send/Cancel — form disappears."""
    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {"template": template,
                   "title": {"tag": "plain_text", "content": "✉️ Reply All"}},
        "body": {"elements": [_md(text)]},
    }


def _on_card_action(data):
    """card.action.trigger over the persistent connection (Send / Cancel)."""
    from lark_oapi.event.callback.model.p2_card_action_trigger import (
        P2CardActionTriggerResponse,
    )

    def respond(kind: str, text: str, card: Optional[dict[str, Any]] = None):
        body: dict[str, Any] = {"toast": {"type": kind, "content": text}}
        if card is not None:
            body["card"] = {"type": "raw", "data": card}
        return P2CardActionTriggerResponse(body)

    try:
        action = getattr(data.event, "action", None)
        value = getattr(action, "value", None) or {}
        if not isinstance(value, dict):
            try:
                value = json.loads(str(value))
            except Exception:
                value = {}
        batch_id = str(value.get("batch") or "")
        if not batch_id:
            return respond("info", "Nothing to do")
        act = str(value.get("action") or "send")
        if act == "cancel":
            batch = _reply_batch_claim(batch_id, "cancelled")
            if batch is None:
                return respond("warning", "Already sent or cancelled")
            for s in batch["specs"]:
                s["quote_html"] = ""
                s["quote_body"] = ""
            return respond("success", "Cancelled — nothing was sent",
                           _reply_done_card("❌ **Cancelled** — no email was sent.", "red"))
        form = getattr(action, "form_value", None) or {}
        if not isinstance(form, dict):
            form = {}
        content = str(form.get("reply_content") or "").strip()
        if not content:
            return respond("error", "Please fill in the content first")
        batch = _reply_batch_claim(batch_id)
        if batch is None:
            return respond("warning", "This reply was already sent (or expired) — run /reply again")
        threading.Thread(target=_send_reply_batch, args=(batch, content), daemon=True).start()
        n = len(batch["specs"])
        return respond("success", f"Sending {n} repl(y/ies)…",
                       _reply_done_card(
                           f"📤 **Sending {n} repl(y/ies)…** — the result summary "
                           "will be posted in this chat.", "green"))
    except Exception as ex:
        print(f"[reply] card action failed: {ex!r}", flush=True)
        return respond("error", "Failed — check the bot logs")


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
    "/csupdate + one email title per line — AI reads each FULL thread (images too)\n"
    "  and explains the issue, solution, status, and if it NEEDS UPDATE\n"
    "/searchwithai + one email title per line — AI reads only the LATEST message\n"
    "  of each and summarizes it (faster than /csupdate)\n"
    "/searchwithoutai + one email title per line — shows just the LATEST message\n"
    "  (content + images), no AI\n"
    "/reply + one email title per line — shows every email's reply-all To/Cc in\n"
    "  a card; fill in ONE content and press Send to reply-all to each of them\n"
    "/machine <name(s) or game type> — PROD machine status card from the live scrape,\n"
    "  split into 🟢 Open to players (online/occupy, no maintain, no test) vs 🛠️ Maintenance\n"
    "  machine. Digits work (/machine 2205); a game type lists all its machines\n"
    "  (/machine man fu bao); a venue filters (/machine mdr bao zhu zhao fu, /machine mdr);\n"
    "  /machine games lists every game type with counts (also per venue: /machine mdr games);\n"
    "  other envs: /machine qat NWR2205 · /machine all 2205\n"
    "/whoami — your open_id + this chat_id (\"who am i\" works when tagged)\n"
    "/deploy — git pull origin main + restart the bot service\n"
    "  (natural text works too: \"git pull and restart service\")\n"
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
    lines.append(f"Machine scrape: {_machine_scrape_diag()}")
    return "\n".join(lines)


def _do_scan_command(chat_id: str, message_id: str) -> None:
    reply_text(chat_id, message_id, "⏳ Scanning mailbox…")
    try:
        seen = scan_mailbox()
        reply_text(chat_id, message_id,
                   f"✅ Scan done — {seen} emails indexed in the {WINDOW_DAYS}-day window.")
    except Exception as ex:
        reply_text(chat_id, message_id, f"❌ Scan failed: {ex!r}")


# ===================== /deploy — git pull origin main + restart the systemd service =====================
# Mirrored from machine bot: `/deploy`, `/gitpullrestart`, or natural text like
# "git pull and restart service" (also 重启 variants). Gated by DEPLOY_ALLOWED_OPEN_IDS.
TOBOT_SERVICE = (_env("TOBOT_SERVICE", default="tobot") or "tobot").strip() or "tobot"
_DEPLOY_ALLOWED_OPEN_IDS = {
    x.strip() for x in (_env("DEPLOY_ALLOWED_OPEN_IDS") or "").split(",") if x.strip()
}


def _deploy_allowed(sender_open_id: str) -> bool:
    """Empty allowlist = anyone who can address the bot may deploy; otherwise restrict to it."""
    if not _DEPLOY_ALLOWED_OPEN_IDS:
        return True
    return (sender_open_id or "").strip() in _DEPLOY_ALLOWED_OPEN_IDS


def _looks_like_deploy_command(text: str) -> bool:
    """Match ``git pull origin main and restart service`` / ``/deploy`` / ``/gitpullrestart``."""
    t = (text or "").strip().casefold()
    if not t:
        return False
    if t in ("/deploy", "/gitpullrestart") or t.startswith("/deploy ") or t.startswith(
        "/gitpullrestart "
    ):
        return True
    has_pull = bool(re.search(r"\bgit\s+pull\b", t)) or bool(
        re.search(r"\bpull\s+(?:origin|code|repo|latest)\b", t)
    )
    has_restart = bool(re.search(r"\b(?:restart|reboot)\b", t)) or "重启" in t
    if has_pull and has_restart:
        return True
    return bool(re.search(r"拉代码.*重启|部署.*重启", t))


def _schedule_service_restart(delay_sec: float = 2.0) -> None:
    """Restart the systemd unit from a DETACHED process so it survives this process exiting."""
    try:
        subprocess.Popen(
            ["bash", "-c", f"sleep {delay_sec}; systemctl restart {TOBOT_SERVICE}"],
            start_new_session=True,
        )
        print(f"[deploy] scheduled: systemctl restart {TOBOT_SERVICE} (in {delay_sec}s)", flush=True)
    except Exception as exc:
        print(f"[deploy] restart schedule failed: {exc!r}", flush=True)


def _do_deploy(chat_id: str, message_id: str) -> None:
    reply_text(chat_id, message_id, f"⏳ `git pull origin main` + restart `{TOBOT_SERVICE}`…")
    try:
        proc = subprocess.run(
            ["git", "pull", "origin", "main"],
            cwd=_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except Exception as exc:
        reply_text(chat_id, message_id, f"❌ `git pull origin main` failed: {exc!r}")
        return
    out = "\n".join(x for x in (proc.stdout, proc.stderr) if x).strip()
    tail = out[-1200:] if len(out) > 1200 else out
    if proc.returncode != 0:
        reply_text(
            chat_id, message_id,
            f"❌ `git pull origin main` failed (exit {proc.returncode}).\n{tail or '(no output)'}",
        )
        return
    reply_text(
        chat_id, message_id,
        f"✅ `git pull origin main` OK — restarting `{TOBOT_SERVICE}`…\n{tail or 'Already up to date.'}",
    )
    _schedule_service_restart()


# ===================== /machine — status card from webmachine_data.json =====================
# Answers straight from the snapshot the background scrape (webmachine.py +
# smmachine.py) keeps fresh — instant reply, no browser is launched here.

_MACHINE_ENV_ORDER = {"PROD": 0, "QAT": 1, "UAT": 2}
_MACHINE_MATCH_CAP = 600   # absolute safety cap across all cards
_MACHINE_PAGE = 150        # machines per card; more matches roll into follow-up cards
_MACHINE_ENV_KEYWORDS = {"prod": "PROD", "qat": "QAT", "uat": "UAT", "all": "ALL"}
# Looks like a machine identifier: NCH1200 / WF8123 / bare 1397 …
_MACHINE_ID_RE = re.compile(r"^[A-Za-z]{1,8}\d{2,6}$|^\d{3,6}$")
_MACHINE_USAGE = (
    "Usage: /machine <name(s)> — e.g. `/machine NWR2205`, digits work too "
    "(`/machine 2205`); separate several queries with commas or new lines.\n"
    "A game type works too: `/machine Standalone` or `/machine man fu bao` lists all its "
    "machines (a line is matched as one phrase — machine names first, then game types).\n"
    "A venue filters: `/machine mdr bao zhu zhao fu` (that game at MDR), `/machine mdr` "
    "(whole venue), `/machine mdr games` (game types at MDR).\n"
    "Paste a list too — on a line like `NCH1200 Red Festival` only the machine ID is "
    "looked up (a bare number like `1397` also works); the game words are ignored.\n"
    "`/machine games` lists every game type with machine counts.\n"
    "Shows PROD only; start with an environment to switch: "
    "`/machine qat NWR2205`, `/machine uat 2205`, `/machine all NWR2205`."
)


def _machine_alnum(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", s or "").upper()


def _machine_data_path() -> str:
    try:
        from webmachine import _data_json_path

        return str(_data_json_path())
    except Exception:
        return os.path.join(_ROOT, "webmachine_data.json")


def _machine_load_rows() -> tuple[list[dict[str, Any]], str, float]:
    """(rows, data path, file mtime) — empty rows + mtime 0.0 when no snapshot yet."""
    path = _machine_data_path()
    try:
        mtime = os.path.getmtime(path)
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, ValueError):
        return [], path, 0.0
    rows = raw if isinstance(raw, list) else []
    return [r for r in rows if isinstance(r, dict)], path, mtime


def _machine_match_rows(queries: list[str],
                        rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Per query: machine-name match first (exact alnum, else substring — so `2205`
    finds NWR2205); when no name matches, fall back to game type the same way
    (so `Standalone` lists every Standalone machine). A multi-word query is tried
    as ONE phrase first (`man fu bao` → game ManFuBao); only when the phrase
    matches nothing is it split into per-word lookups."""
    named, gamed = [], []
    for r in rows:
        na = _machine_alnum(str(r.get("name") or ""))
        ga = _machine_alnum(str(r.get("game_type") or ""))
        if na:
            named.append((na, r))
        if ga:
            gamed.append((ga, r))

    def _hits_for(ta: str, include_games: bool = True) -> list[dict[str, Any]]:
        h = [r for na, r in named if na == ta] or [r for na, r in named if ta in na]
        if not h and include_games:
            h = [r for ga, r in gamed if ga == ta] or [r for ga, r in gamed if ta in ga]
        return h

    matched: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    not_found: list[str] = []

    def _add(hits: list[dict[str, Any]]) -> None:
        for r in hits:
            key = (str(r.get("environment")), str(r.get("belongs")), str(r.get("name")))
            if key not in seen:
                seen.add(key)
                matched.append(r)

    for q in queries:
        qa = _machine_alnum(q)
        if not qa:
            continue
        hits = _hits_for(qa)
        if hits:
            _add(hits)
            continue
        # Word fallback matches machine NAMES only — never game types, so a
        # phrase like `Bao Zhu Zhao Fu` can't leak machines of other games
        # via `bao`/`fu` substrings. When the line contains machine-ID-looking
        # words (`NCH1200 Red Festival` or `1397 Purple Celebration`), ONLY the
        # IDs are looked up — the game-name words are pasted commentary.
        words = [w for w in q.split() if _machine_alnum(w)]
        if len(words) > 1:
            id_words = [w for w in words if _MACHINE_ID_RE.match(_machine_alnum(w))]
            missed_words: list[str] = []
            any_hit = False
            for w in (id_words or words):
                wh = _hits_for(_machine_alnum(w), include_games=False)
                if wh:
                    _add(wh)
                    any_hit = True
                else:
                    missed_words.append(w)
            if id_words or any_hit:
                not_found.extend(missed_words)
            else:
                not_found.append(q)  # whole phrase missed — report it as one miss
        else:
            not_found.append(q)
    matched.sort(key=lambda r: (
        _MACHINE_ENV_ORDER.get(str(r.get("environment") or "").upper(), 9),
        str(r.get("belongs") or "").lower(),
        str(r.get("name") or "").lower(),
    ))
    return matched, not_found


def _machine_row_env(r: dict[str, Any]) -> str:
    return str(r.get("environment") or "PROD").strip().upper() or "PROD"


def _machine_is_open(r: dict[str, Any]) -> bool:
    """Open to players = online AND status normal/occupy AND not maintain AND not test.

    Everything else (offline, maintain, test, unknown status) counts as a
    maintenance machine.
    """
    online = str(r.get("online") or "").lower()
    if "offline" in online or "online" not in online:
        return False
    if r.get("is_test"):
        return False
    status = str(r.get("status") or "").lower()
    if "maintain" in status:
        return False
    return "normal" in status or "occupy" in status


def _machine_lead_emoji(r: dict[str, Any]) -> str:
    """🟢 open to players; otherwise the reason it counts as maintenance."""
    if _machine_is_open(r):
        return "🟢"
    online = str(r.get("online") or "").lower()
    if "offline" in online or "online" not in online:
        return "🔴"
    if "maintain" in str(r.get("status") or "").lower():
        return "🛠️"
    if r.get("is_test"):
        return "🧪"
    return "⚪"


def _machine_row_md(r: dict[str, Any]) -> str:
    """One line per machine: the lead emoji (🟢 open / 🔴🛠️🧪 why not) + bold name."""
    name = str(r.get("name") or "—").strip()
    return f"{_machine_lead_emoji(r)} **{name}**"


def _machine_age_label(mtime: float) -> str:
    age = max(0, int(time.time() - mtime))
    if age < 120:
        return f"{age}s ago"
    if age < 7200:
        return f"{age // 60} min ago"
    if age < 172800:
        return f"{age // 3600} h ago"
    return f"{age // 86400} d ago"


def _machine_card(matched: list[dict[str, Any]], not_found: list[str],
                  mtime: float, truncated: int, env_label: str,
                  elsewhere: list[tuple[str, list[str]]],
                  page: Optional[tuple[int, int]] = None,
                  suggests: Optional[list[tuple[str, list[str]]]] = None) -> dict[str, Any]:
    suggests = suggests or []
    open_rows = [r for r in matched if _machine_is_open(r)]
    maint_rows = [r for r in matched if not _machine_is_open(r)]
    misses = len(not_found) + len(elsewhere) + len(suggests)
    if matched and not misses and not maint_rows:
        template = "green"
    elif not matched or not open_rows:
        template = "red"
    else:
        template = "orange"
    title = f"🎰 Machine status ({env_label}) — {len(open_rows)} open, {len(maint_rows)} maintenance"
    if misses:
        title += f", {misses} not found"
    if page:
        title += f" · page {page[0]}/{page[1]}"
    elements: list[dict[str, Any]] = []

    def _section_md(header: str, rows_: list[dict[str, Any]]) -> str:
        by_env: dict[str, list[str]] = {}
        for r in rows_:
            by_env.setdefault(_machine_row_env(r), []).append(_machine_row_md(r))
        parts = [header]
        for env in sorted(by_env, key=lambda e: _MACHINE_ENV_ORDER.get(e, 9)):
            if env_label.startswith("ALL"):
                parts.append(f"📍 **{env}**")
            parts.extend(by_env[env])
        return "\n".join(parts)

    open_md = _section_md(f"🟢 **Open to players ({len(open_rows)})**", open_rows) if open_rows else ""
    maint_md = _section_md(f"🛠️ **Maintenance machine ({len(maint_rows)})**", maint_rows) if maint_rows else ""
    if open_md and maint_md:
        # Both groups: open on the left, maintenance on the right.
        elements.append({
            "tag": "column_set",
            "flex_mode": "bisect",
            "background_style": "default",
            "columns": [
                {"tag": "column", "width": "weighted", "weight": 1, "vertical_align": "top",
                 "elements": [{"tag": "markdown", "content": open_md}]},
                {"tag": "column", "width": "weighted", "weight": 1, "vertical_align": "top",
                 "elements": [{"tag": "markdown", "content": maint_md}]},
            ],
        })
    elif open_md or maint_md:
        elements.append({"tag": "markdown", "content": open_md or maint_md})
    if truncated > 0:
        elements.append({"tag": "markdown",
                         "content": f"*… and {truncated} more matches not shown*"})
    if not_found:
        where = f" in {env_label}" if env_label != "ALL" else ""
        elements.append({"tag": "markdown",
                         "content": f"❓ **Not found{where}:** " + ", ".join(not_found[:40])})
    for tok, envs in elsewhere[:10]:
        elements.append({"tag": "markdown",
                         "content": (f"💡 **{tok}** is not in {env_label} but exists in "
                                     f"{', '.join(envs)} — try `/machine {envs[0].lower()} {tok}`")})
    for q, sims in suggests[:10]:
        elements.append({"tag": "markdown",
                         "content": (f"💡 No match for **{q}** — similar game types: "
                                     f"{', '.join(sims)} (see `/machine games`)")})
    elements.append({"tag": "markdown",
                     "content": f"🕒 *Updated {_machine_age_label(mtime)}*"})
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": title[:150]},
            "template": template,
        },
        "elements": elements,
    }


def _machine_scrape_diag() -> str:
    """Live diagnosis of the background scrape (same process, so state is exact)."""
    try:
        import webmachine as _wm
    except Exception as ex:
        return f"scrape module unavailable: {ex!r}"
    if not _wm._scrape_enabled():
        return "scrape is DISABLED (WEBMACHINE_SCRAPE=0 in .env)"
    alive = any(th.name == "webmachine-scrape" for th in threading.enumerate())
    with _wm._scrape_lock:
        ts = _wm._scrape_ts
        errs = dict(_wm._scrape_errs)
        nrows = len(_wm._scrape_rows)
    if not ts:
        if alive:
            return "loop running, first scrape pass hasn't finished yet"
        return ("loop thread NOT running — the bot started without it; "
                "check startup logs for '[webmachine]' lines and restart")
    raw_n = getattr(_wm, "_scrape_raw_count", -1)
    counts = dict(getattr(_wm, "_scrape_counts", {}) or {})
    head = f"last pass {_machine_age_label(ts)}: {nrows} machines, {len(errs)} backend notes"
    if raw_n >= 0 and raw_n != nrows:
        head += f" (scraper returned {raw_n} raw rows)"
    if errs:
        shown = [f"• {k}: {str(v)[:160]}" for k, v in list(errs.items())[:8]]
        if len(errs) > 8:
            shown.append(f"• … +{len(errs) - 8} more")
        head += "\n" + "\n".join(shown)
    if counts:
        top = sorted(counts.items(), key=lambda kv: -kv[1])[:8]
        head += "\nrows per backend: " + ", ".join(f"{k}={v}" for k, v in top)
    elif raw_n == 0:
        head += ("\nEvery backend logged in without error but returned an EMPTY machine table — "
                 "check the EGM list path (SM_MACHINE_PATH, default /egm/egmStatusList) and that "
                 "the backend accounts can see machines. Test one backend on the server:\n"
                 "  python -c \"import smmachine;r,w=smmachine.smachine_collect_all_machine_rows('cp');"
                 "print(len(r),w)\"")
    return head


def _do_machine(chat_id: str, message_id: str, arg: str) -> None:
    raw = (arg or "").strip()
    env_sel = "PROD"
    first = raw.split(None, 1)
    if first and first[0].lower() in _MACHINE_ENV_KEYWORDS:
        env_sel = _MACHINE_ENV_KEYWORDS[first[0].lower()]
        raw = first[1] if len(first) > 1 else ""
    rows, path, mtime = _machine_load_rows()
    # Optional venue filter next (belongs values from the data: mdr, nwr, cp, …):
    # `/machine mdr bao zhu zhao fu` = that game at MDR only; `/machine mdr` = whole venue.
    venue_sel = ""
    venues = {_machine_alnum(str(r.get("belongs") or "")) for r in rows}
    venues.discard("")
    first = raw.split(None, 1)
    if first and _machine_alnum(first[0]) in venues:
        venue_sel = _machine_alnum(first[0])
        raw = first[1] if len(first) > 1 else ""
    # One query per line (or comma) — a line is tried as a whole phrase first,
    # so `/machine man fu bao` is ONE game-type lookup, not three words.
    queries = [q.strip() for q in re.split(r"[\n,;，；]+", raw) if q.strip()]
    if not queries and not venue_sel:
        reply_text(chat_id, message_id, _MACHINE_USAGE)
        return
    if not rows:
        if mtime <= 0:
            head = (f"⚠️ No machine snapshot yet — {os.path.basename(path)} doesn't exist, "
                    "so no scrape pass has completed since startup.")
        else:
            head = (f"⚠️ The last scrape pass found 0 machines — {os.path.basename(path)} "
                    f"was updated {_machine_age_label(mtime)} but is empty (usually every "
                    "backend failed to launch a browser or log in).")
        reply_text(chat_id, message_id, head + "\n\nScrape diagnosis:\n" + _machine_scrape_diag())
        return
    def _in_scope(r: dict[str, Any]) -> bool:
        if env_sel != "ALL" and _machine_row_env(r) != env_sel:
            return False
        if venue_sel and _machine_alnum(str(r.get("belongs") or "")) != venue_sel:
            return False
        return True

    pool = [r for r in rows if _in_scope(r)]
    scope_label = f"{env_sel} · {venue_sel}" if venue_sel else env_sel
    # `/machine games` — list every game type with machine counts (to find exact names).
    if len(queries) == 1 and queries[0].lower() in ("games", "game types", "gametypes", "game type"):
        stats: dict[str, list[Any]] = {}
        for r in pool:
            g = str(r.get("game_type") or "").strip()
            ga = _machine_alnum(g)
            if not ga:
                continue
            s = stats.setdefault(ga, [g, 0, 0])
            s[1] += 1
            s[2] += 1 if _machine_is_open(r) else 0
        ordered = sorted(stats.values(), key=lambda s: (-s[1], s[0].lower()))
        lines = [f"🕹️ **{disp}** — {total} machines ({op} open)"
                 for disp, total, op in ordered[:100]]
        if len(ordered) > 100:
            lines.append(f"*… and {len(ordered) - 100} more game types*")
        lines.append(f"🕒 *Updated {_machine_age_label(mtime)}*")
        reply_card(chat_id, message_id, {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text",
                          "content": f"🎰 Game types ({scope_label}) — {len(ordered)} games, "
                                     f"{sum(s[1] for s in ordered)} machines"[:150]},
                "template": "blue",
            },
            "elements": [{"tag": "markdown", "content": "\n".join(lines)}],
        })
        return
    if queries:
        matched, not_found = _machine_match_rows(queries, pool)
    else:
        # Venue alone (`/machine mdr`) — every machine of that venue.
        matched = sorted(pool, key=lambda r: (
            _MACHINE_ENV_ORDER.get(str(r.get("environment") or "").upper(), 9),
            str(r.get("belongs") or "").lower(),
            str(r.get("name") or "").lower(),
        ))
        not_found = []
    # Misses that DO exist outside the selected scope get a hint instead of "not found".
    elsewhere: list[tuple[str, list[str]]] = []
    if not_found and (env_sel != "ALL" or venue_sel):
        other_rows = [r for r in rows if not _in_scope(r)]
        hard_missing: list[str] = []
        for tok in not_found:
            m2, _ = _machine_match_rows([tok], other_rows)
            if m2:
                envs = sorted({_machine_row_env(r) for r in m2},
                              key=lambda e: _MACHINE_ENV_ORDER.get(e, 9))
                elsewhere.append((tok, envs))
            else:
                hard_missing.append(tok)
        not_found = hard_missing
    # For remaining misses, suggest game types sharing a word (≥2 chars) with the query.
    suggests: list[tuple[str, list[str]]] = []
    if not_found:
        games: dict[str, str] = {}
        for r in pool:
            g = str(r.get("game_type") or "").strip()
            ga = _machine_alnum(g)
            if g and ga:
                games.setdefault(ga, g)
        hard_missing = []
        for q in not_found:
            words = [w for w in (_machine_alnum(x) for x in q.split()) if len(w) >= 2]
            sims = [disp for ga, disp in games.items() if any(w in ga for w in words)]
            if sims:
                suggests.append((q, sims[:5]))
            else:
                hard_missing.append(q)
        not_found = hard_missing
    shown = matched[:_MACHINE_MATCH_CAP]
    over = len(matched) - len(shown)
    pages = [shown[i:i + _MACHINE_PAGE] for i in range(0, len(shown), _MACHINE_PAGE)] or [[]]
    for pi, page_rows in enumerate(pages, 1):
        last = pi == len(pages)
        card = _machine_card(
            page_rows,
            not_found if last else [],
            mtime,
            truncated=over if last else 0,
            env_label=scope_label,
            elsewhere=elsewhere if last else [],
            page=(pi, len(pages)) if len(pages) > 1 else None,
            suggests=suggests if last else None,
        )
        reply_card(chat_id, message_id, card)


def _process_message(text: str, chat_id: str, message_id: str, directed: bool,
                     sender_id: str = "") -> None:
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
    # The /searchwith* variants must be tested before plain /search — each has
    # "/search" as a prefix and would otherwise be swallowed by it.
    if _looks_like_deploy_command(t) and (
        directed or low.startswith(("/deploy", "/gitpullrestart"))
    ):
        if _deploy_allowed(sender_id):
            action = lambda: _do_deploy(chat_id, message_id)
        else:
            action = lambda: reply_text(chat_id, message_id,
                                        "❌ You are not allowed to deploy this bot.")
    elif low.startswith("/searchwithoutai"):
        action = lambda: _do_searchwithoutai(chat_id, message_id, t[len("/searchwithoutai"):])
    elif low.startswith("/searchwithai"):
        action = lambda: _do_searchwithai(chat_id, message_id, t[len("/searchwithai"):])
    elif low.startswith("/search"):
        action = lambda: _search_and_reply(chat_id, message_id, t[len("/search"):])
    elif low.startswith("/csupdate"):
        action = lambda: _do_csupdate(chat_id, message_id, t[len("/csupdate"):])
    elif low.startswith("/reply"):
        action = lambda: _do_reply(chat_id, message_id, t[len("/reply"):])
    elif low.startswith("/machine"):
        action = lambda: _do_machine(chat_id, message_id, t[len("/machine"):])
    elif low in ("/whoami", "/myid") or (
        directed and re.fullmatch(r"who\s*am\s*i\s*\??|my\s*(?:open[\s_]*)?id\s*\??", low)
    ):
        action = lambda: reply_text(
            chat_id, message_id,
            f"👤 Your open_id: {sender_id or 'unknown'}\nchat_id: {chat_id}\n"
            "(use the open_id for DEPLOY_ALLOWED_OPEN_IDS in .env)",
        )
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
    # open_id printed for every command (needed for DEPLOY_ALLOWED_OPEN_IDS).
    print(f"👤 [tobot] open_id={sender_id or '?'} chat_id={chat_id} cmd={t[:60]!r}", flush=True)
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
        sender_id = (getattr(getattr(getattr(data.event, "sender", None), "sender_id", None),
                             "open_id", "") or "")
        threading.Thread(
            target=_process_message, args=(text, chat_id, message_id, directed, sender_id),
            daemon=True,
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
        .register_p2_card_action_trigger(_on_card_action)
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
    # Machine scrape (copied from machine bot): warm browser per backend/environment,
    # walks every machine in PROD/QAT/UAT and stores webmachine_data.json.
    # SystemExit included: smmachine raises it for a bad WEBMACHINE_SITES alias,
    # and a scrape-config typo must not take down the email bot.
    try:
        import webmachine as _wm

        _wm.start_background_scrape_loop()
    except (Exception, SystemExit) as ex:
        print(f"[webmachine] scrape loop not started: {ex!r}", flush=True)
    try:
        import smmachine as _boot_wm

        _boot_wm.prewarm_webmachine_scrape_pool_on_startup()
    except (Exception, SystemExit) as ex:
        print(f"[webmachine] warm pool prewarm skipped: {ex!r}", flush=True)
    run_ws_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
