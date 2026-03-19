"""
koyeb_app/main.py - YouTube Upload Scheduler
Polling GDrive setiap 1 menit, upload ke YouTube kalau sudah waktunya.
HTTP server di port 8080 untuk keep-alive ping dari Uptime.com
"""

import os
import json
import time
import pickle
import base64
import tempfile
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("uploader")

POLL_INTERVAL_SEC = 1 * 60  # 1 menit
GDRIVE_ROOT       = "mesin_cuan"
QUEUE_FOLDER      = "queue"
DONE_FOLDER       = "done"

CHANNELS = [
    {"id": "ch_id_horror", "env_key": "TOKEN_CH_ID_HORROR", "language": "id"},
    {"id": "ch_id_psych",  "env_key": "TOKEN_CH_ID_PSYCH",  "language": "id"},
    {"id": "ch_en_horror", "env_key": "TOKEN_CH_EN_HORROR", "language": "en"},
    {"id": "ch_en_psych",  "env_key": "TOKEN_CH_EN_PSYCH",  "language": "en"},
]

# Status untuk health check
_status = {"last_poll": None, "uploads_today": 0, "running": True}


# ─── HTTP Keep-alive server ───────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        resp = json.dumps({
            "status":       "running",
            "last_poll":    _status["last_poll"],
            "uploads_today": _status["uploads_today"],
        })
        self.wfile.write(resp.encode())

    def log_message(self, format, *args):
        pass  # Suppress HTTP logs


def _start_health_server():
    port   = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Health server jalan di port {port}")
    server.serve_forever()


# ─── Token helpers ────────────────────────────────────────────────────────────

def _load_creds_from_env(env_key: str):
    b64 = os.environ.get(env_key)
    if not b64:
        raise EnvironmentError(f"Env variable '{env_key}' tidak ditemukan.")
    raw   = base64.b64decode(b64.encode("utf-8"))
    creds = pickle.loads(raw)
    if creds.expired and creds.refresh_token:
        from google.auth.transport.requests import Request
        creds.refresh(Request())
    return creds


def _get_drive_service(creds):
    from googleapiclient.discovery import build
    return build("drive", "v3", credentials=creds)


def _get_youtube_service(creds):
    from googleapiclient.discovery import build
    return build("youtube", "v3", credentials=creds)


# ─── GDrive helpers ───────────────────────────────────────────────────────────

def _find_folder(service, name: str, parent_id) -> str | None:
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id)").execute()
    files   = results.get("files", [])
    return files[0]["id"] if files else None


def _ensure_folder(service, name: str, parent_id) -> str:
    folder_id = _find_folder(service, name, parent_id)
    if folder_id:
        return folder_id
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        meta["parents"] = [parent_id]
    return service.files().create(body=meta, fields="id").execute()["id"]


def _list_subfolders(service, parent_id: str) -> list:
    query   = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)", orderBy="name").execute()
    return results.get("files", [])


def _find_file_in_folder(service, folder_id: str, filename: str) -> str | None:
    query   = f"'{folder_id}' in parents and name='{filename}' and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files   = results.get("files", [])
    return files[0]["id"] if files else None


def _download_file(service, file_id: str, dest_path: str):
    from googleapiclient.http import MediaIoBaseDownload
    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def _move_to_done(service, folder_id: str, done_root_id: str, ch_id: str):
    ch_done    = _ensure_folder(service, ch_id, done_root_id)
    file_info  = service.files().get(fileId=folder_id, fields="parents").execute()
    old_parent = file_info.get("parents", [""])[0]
    service.files().update(
        fileId=folder_id,
        addParents=ch_done,
        removeParents=old_parent,
        fields="id, parents"
    ).execute()


# ─── YouTube upload ───────────────────────────────────────────────────────────

def _upload_to_youtube(yt_service, video_path: str, thumbnail_path: str, metadata: dict) -> str:
    from googleapiclient.http import MediaFileUpload

    language = metadata.get("language", "id")
    body = {
        "snippet": {
            "title":                metadata["title"],
            "description":          metadata["description"],
            "tags":                 metadata.get("tags", []),
            "categoryId":           metadata.get("category_id", "27"),
            "defaultLanguage":      language,
            "defaultAudioLanguage": language,
        },
        "status": {
            "privacyStatus":           "public",
            "selfDeclaredMadeForKids": metadata.get("made_for_kids", False),
            "embeddable":              True,
            "publicStatsViewable":     True,
            "license":                 "youtube",
            "containsSyntheticMedia":  metadata.get("contains_ai_content", True),
        },
        "recordingDetails": {
            "recordingDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00.000Z")
        }
    }

    media   = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True, chunksize=5*1024*1024)
    request = yt_service.videos().insert(part="snippet,status,recordingDetails", body=body, media_body=media)

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            logger.info(f"Upload progress: {int(status.progress()*100)}%")

    video_id = response["id"]
    url      = f"https://www.youtube.com/watch?v={video_id}"

    if thumbnail_path and os.path.exists(thumbnail_path):
        try:
            media_thumb = MediaFileUpload(thumbnail_path, mimetype="image/png")
            yt_service.thumbnails().set(videoId=video_id, media_body=media_thumb).execute()
        except Exception as e:
            logger.warning(f"Thumbnail gagal: {e}")

    return url


# ─── Main polling ─────────────────────────────────────────────────────────────

def process_queue():
    now_utc = datetime.now(timezone.utc)
    logger.info(f"Polling — {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    _status["last_poll"] = now_utc.isoformat()

    for ch in CHANNELS:
        ch_id   = ch["id"]
        env_key = ch["env_key"]

        try:
            creds   = _load_creds_from_env(env_key)
            drive   = _get_drive_service(creds)
            youtube = _get_youtube_service(creds)

            root_id  = _find_folder(drive, GDRIVE_ROOT, None)
            if not root_id: continue
            queue_id = _find_folder(drive, QUEUE_FOLDER, root_id)
            if not queue_id: continue
            ch_queue = _find_folder(drive, ch_id, queue_id)
            if not ch_queue: continue

            subfolders = _list_subfolders(drive, ch_queue)
            if not subfolders:
                continue

            done_id = _ensure_folder(drive, DONE_FOLDER, root_id)

            for folder in subfolders:
                folder_id   = folder["id"]
                folder_name = folder["name"]

                # Download metadata
                meta_id = _find_file_in_folder(drive, folder_id, "metadata.json")
                if not meta_id:
                    continue

                with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
                    meta_path = tmp.name
                _download_file(drive, meta_id, meta_path)
                with open(meta_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
                os.remove(meta_path)

                if metadata.get("status") != "ready":
                    continue

                # Cek jadwal publish
                publish_at = metadata.get("publish_at", "")
                if publish_at:
                    try:
                        pub_dt = datetime.fromisoformat(publish_at.replace("Z", "+00:00"))
                        if pub_dt > now_utc:
                            logger.info(f"[{ch_id}] {folder_name} belum waktunya, skip.")
                            continue
                    except Exception:
                        pass

                # Download video
                video_id_gdrive = _find_file_in_folder(drive, folder_id, "video.mp4")
                if not video_id_gdrive:
                    continue

                logger.info(f"[{ch_id}] Downloading: {folder_name}...")
                with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                    video_path = tmp.name
                _download_file(drive, video_id_gdrive, video_path)

                # Download thumbnail
                thumb_path    = None
                thumb_id      = _find_file_in_folder(drive, folder_id, "thumbnail.png")
                if thumb_id:
                    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                        thumb_path = tmp.name
                    _download_file(drive, thumb_id, thumb_path)

                # Upload ke YouTube
                logger.info(f"[{ch_id}] Uploading: {metadata['title']}")
                try:
                    url = _upload_to_youtube(youtube, video_path, thumb_path, metadata)
                    logger.info(f"[{ch_id}] ✅ {url}")
                    _status["uploads_today"] += 1
                    _move_to_done(drive, folder_id, done_id, ch_id)
                except Exception as e:
                    logger.error(f"[{ch_id}] Upload gagal: {e}")
                finally:
                    if os.path.exists(video_path): os.remove(video_path)
                    if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)

                time.sleep(60)  # Jeda 60 detik antar upload

        except Exception as e:
            logger.error(f"[{ch_id}] Error: {e}")


def main():
    logger.info("🚀 YouTube Uploader dimulai")
    logger.info(f"   Polling setiap {POLL_INTERVAL_SEC} detik")

    # Start HTTP server di thread terpisah (untuk Uptime.com ping)
    t = threading.Thread(target=_start_health_server, daemon=True)
    t.start()

    while True:
        try:
            process_queue()
        except Exception as e:
            logger.error(f"Polling error: {e}")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()