# TObot — Lark Email Search Bot 📮

Indexes **all emails** from the duty mailbox (mirrored from osedutybot's
`allemail.json` design, plus stored bodies) and lets you search them from any
Lark chat.

## Commands

| Command | What it does |
|---|---|
| `/search <email title>` | Fuzzy search by subject — lists matches with their Message-IDs |
| `/search <exact full title>` | Thread mode: ONE card with the whole conversation — the original + every Re:/Fwd: reply, oldest first (similar-but-different titles excluded, identical contents deduped) |
| `/search <Message-ID>` | Exact lookup (most accurate) — shows sender, To/Cc, date, folder and content |
| `/search <No.>` | Open result N from your previous `/search` listing |
| `@TObot <email title>` | Same as `/search` (in P2P chat just type the title) |
| `/csupdate` + one email title per line | AI review of the **whole thread** (qwen): reads every message **and its images**, explains the issue, the solution if present, the current status, and flags `⚠️ NEEDS UPDATE` when the newest message is ≥2 days old and unresolved |
| `/searchwithai` + one email title per line | AI review of the **latest message only** (qwen): faster — summary, status, and freshness of just the newest email in each thread |
| `/searchwithoutai` + one email title per line | Just the **latest message** (sender + content + images), **no AI** — the quick "what's the newest reply" view |
| `/reply` + one email title per line | All-or-nothing reply-all: every title must be found, then ONE card shows each thread's own To/Cc (reply-all to its latest message) with an input box — fill in one content, press **Send**, and each thread gets the reply with its own recipients. Sent from `om@` via SMTP with proper threading (In-Reply-To/References) |
| `/machine <name(s)>` | **PROD** machine status card: **🟢 Open to players** (online/occupy, no maintain, no test) on the **left**, **🛠️ Maintenance machine** (everything else) on the **right** — one line per machine, the lead emoji says why it's in maintenance (🔴 offline / 🛠️ maintain / 🧪 test). Digits work (`/machine 2205` finds NWR2205), a **game type** lists all its machines (`/machine man fu bao` — names are matched first, then game types, exact before substring), a **venue** filters (`/machine mdr bao zhu zhao fu`, `/machine mdr` = whole venue, `/machine mdr games`), `/machine games` lists every game type with counts, several queries per message OK — paste a whole list and on lines like `NCH1200 Red Festival` only the machine ID is looked up (bare numbers like `1397` work too, game words are ignored); misses that exist elsewhere get a 💡 hint, unknown phrases get similar-game suggestions. Prefix with `qat` / `uat` / `all` for other environments. Header goes green (all open) / orange (mixed) / red (none open). Answers instantly from `webmachine_data.json`, kept fresh by the background scrape |
| `/scan` | Force an immediate mailbox re-scan |
| `/status` | Index size, retention window, last scan (per folder) |
| `/help` | Help |

While TObot processes your message it reacts with **GotIt** 👌 on it, then swaps
to **Done** ✅ when the reply is sent (needs the *message reactions* permission
in the Lark developer console).

Typical flow: `/search evolution maintenance` → pick from the listing →
`/search 2` (or paste the Message-ID for the exact email).

## How it works

- The index tracks **email title → Message-ID** (plus From/To/date/folder).
  A background thread logs into IMAP (`imap.larksuite.com`) every
  `TOBOT_SCAN_INTERVAL_SEC` (default 5 min) and refreshes headers for the last
  `TOBOT_WINDOW_DAYS` (default 180) days into `allemail.json` (atomic writes,
  deduped by Message-ID). No bodies are stored, so scans are fast and the
  index stays small.
- `/search <title>` resolves the title to its Message-ID, then uses that ID to
  retrieve the exact email **live from the mailbox** (by its recorded
  folder/uid, falling back to a Message-ID search if the email moved) and
  shows the full content. Recently opened emails are cached in memory.
- Lark events arrive through a **persistent connection** (WebSocket) — no
  public Request URL / port forwarding needed. In the Lark developer console
  set: *Event subscriptions → Receive events through persistent connection*
  and subscribe to `im.message.receive_v1`.

## Machine scrape (copied from machine bot)

`webmachine.py` + `smmachine.py` + `checkcredit.py` are trimmed copies of the
machine bot's read-only scrape: a **warm browser pool** keeps one logged-in
Chromium open per backend/environment, a background loop walks **every machine
in every environment** (PROD via `*_BACKEND_*` logins; QAT/UAT via
`*.osmslot.org`) every `WEBMACHINE_SCRAPE_INTERVAL_SEC` (default 15 min), and
stores the rows to **`webmachine_data.json`**. It starts automatically with
`main.py`; requires `pip install playwright && playwright install chromium`
plus the backend logins in `.env` (see `.env.example`), and can be disabled
with `WEBMACHINE_SCRAPE=0` / kept one-shot with `WEBMACHINE_WARM_POOL=0`.

On a **server without a display** (the systemd deploy) set
`WEBMACHINE_WARM_HEADLESS=1` — warm-pool browsers are *headed* by default and
headed Chromium cannot start without an X server, so every scrape pass would
fail (and rewrite the JSON as `[]`). Note `BOT_PLAYWRIGHT_HEADLESS=1` covers
only one-shot scrapes, not the warm pool. If TObot and the machine bot ever
run warm pools on the same host they collide on the shared
`$TMPDIR/wm_warm_profile_*` Chromium profiles — keep only one bot's pool on.

## Local run

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in values (or use the real .env)
python main.py
```

## Server deploy (systemd)

```bash
cd /root
git clone https://github.com/mrcodestealer/TObot.git
cd TObot
pip3 install -r requirements.txt
# If pip refuses with "externally-managed-environment" (Ubuntu 23+/Debian 12+):
#   pip3 install --break-system-packages -r requirements.txt
playwright install chromium       # browser for the machine scrape
playwright install-deps chromium  # Chromium shared libs (Debian/Ubuntu)
nano .env                        # paste the real .env (not in git)
cp tobot.service /etc/systemd/system/tobot.service
systemctl daemon-reload
systemctl enable --now tobot.service
journalctl -u tobot.service -f   # watch logs
```

Update on the server:

```bash
cd /root/TObot
git pull
systemctl restart tobot.service
```

## Notes

- `.env` and `allemail.json` are gitignored — sync `.env` to the server by
  copy-paste, never commit it.
- The bot only answers `/`-commands (and `help`), so it is safe in busy groups.
