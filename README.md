# Mesin Cuan Uploader

> ☁️ Cloud-side companion for [Mesin Cuan](https://github.com/algojogacor/mesin-cuan) — polls Google Drive, uploads to YouTube, sends Telegram notifications.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)
[![Deploy to Koyeb](https://img.shields.io/badge/Deploy-Koyeb-4B32C3?style=flat-square)](https://koyeb.com)

---

## What It Does

Part of the **Mesin Cuan two-step architecture**:

```
Mesin Cuan (local) → renders videos → GDrive Queue
                                            ↓
                              Mesin Cuan Uploader (Koyeb)
                              polls GDrive every 5 min
                                            ↓
                                      YouTube Upload
                                      with paced scheduling
```

This separation prevents YouTube from detecting bulk uploads from a single IP — the uploader runs on a cloud IP, paced 60s between uploads.

---

## Features

- **GDrive polling** — checks `mesin_cuan/queue/{channel}/` every 5 minutes
- **YouTube upload** — resumable upload with metadata, tags, thumbnail, AI-content declaration
- **Smart scheduling** — respects `publish_at` timestamps, skips videos not yet due
- **Telegram notifications** — real-time alerts for upload success, failure, errors
- **Daily summary** — midnight UTC recap per channel with upload counts
- **Health check** — HTTP server on port 8080 for Koyeb/Uptime.com keep-alive
- **Auto-retry** — refresh expired OAuth tokens automatically

---

## Quick Deploy

### 1. Prepare OAuth Tokens

On your local machine (where Mesin Cuan runs):

```bash
python setup_auth.py --channel ch_id_horror
```

This generates `data/ch_id_horror/ch_id_horror_token.pickle` — encode it to base64:

```bash
base64 -w0 data/ch_id_horror/ch_id_horror_token.pickle
```

### 2. Deploy to Koyeb / Railway

Set these environment variables:

```env
# Required — base64-encoded OAuth pickle per channel
TOKEN_CH_ID_HORROR=<base64-encoded-token>
TOKEN_CH_ID_PSYCH=<base64-encoded-token>
TOKEN_CH_EN_HORROR=<base64-encoded-token>
TOKEN_CH_EN_PSYCH=<base64-encoded-token>

# Optional — Telegram notifications
TELEGRAM_BOT_TOKEN=<your-bot-token>
TELEGRAM_CHAT_ID=<your-chat-id>

# Port for health check
PORT=8080
```

### 3. Set Up Health Check

Point Uptime.com (or Koyeb's built-in health check) to `https://your-app.koyeb.app/` — returns JSON with upload stats.

---

## Architecture

```
main.py
├── HealthCheckServer (port 8080)  → Uptime.com keep-alive
├── process_queue()                → runs every 5 min
│   ├── iterate CHANNELS
│   ├── _load_creds_from_env()     → decode OAuth from env
│   ├── _find_folder()             → locate GDrive folders
│   ├── _download_file()           → pull video + metadata
│   ├── _upload_to_youtube()       → resumable upload + thumbnail
│   └── _move_to_done()            → archive to done/ folder
├── _check_and_send_daily_summary() → midnight UTC recap
└── _send_telegram()               → HTML-formatted alerts
```

---

## License

MIT — free to use, modify, and distribute.

See [LICENSE](LICENSE) for full text.

---

*Companion to [Mesin Cuan Viral Architect](https://github.com/algojogacor/mesin-cuan)*
