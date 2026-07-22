# TObot — Lark Email Search Bot 📮

Indexes **all emails** from the duty mailbox (mirrored from osedutybot's
`allemail.json` design, plus stored bodies) and lets you search them from any
Lark chat.

## Commands

| Command | What it does |
|---|---|
| `/search <email title>` | Fuzzy search by subject — lists matches with their Message-IDs |
| `/search <Message-ID>` | Exact lookup (most accurate) — shows sender, To/Cc, date, folder and content |
| `/search <No.>` | Open result N from your previous `/search` listing |
| `@TObot <email title>` | Same as `/search` (in P2P chat just type the title) |
| `/scan` | Force an immediate mailbox re-scan |
| `/status` | Index size, retention window, last scan (per folder) |
| `/help` | Help |

While TObot processes your message it reacts with **GotIt** 👌 on it, then swaps
to **Done** ✅ when the reply is sent (needs the *message reactions* permission
in the Lark developer console).

Typical flow: `/search evolution maintenance` → pick from the listing →
`/search 2` (or paste the Message-ID for the exact email).

## How it works

- A background thread logs into IMAP (`imap.larksuite.com`) every
  `TOBOT_SCAN_INTERVAL_SEC` (default 5 min), lists everything in
  `TOBOT_IMAP_FOLDERS` from the last `TOBOT_WINDOW_DAYS` (default 180) days,
  and stores subject / Message-ID / From / To / Cc / date / folder / body
  into `allemail.json` (atomic writes, deduped by Message-ID).
- Bodies are fetched **only for new emails** (bounded per scan), so scans stay cheap.
- Lark events arrive through a **persistent connection** (WebSocket) — no
  public Request URL / port forwarding needed. In the Lark developer console
  set: *Event subscriptions → Receive events through persistent connection*
  and subscribe to `im.message.receive_v1`.

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
