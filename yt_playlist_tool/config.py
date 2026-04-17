"""Uygulama genelinde kullanılan sabitler ve tema ayarları."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path


SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
REQUEST_TIMEOUT_SECONDS = 25
MAX_API_RESULTS = 50
RETRY_TOTAL = 3
RETRY_BACKOFF_FACTOR = 0.7
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
APP_TITLE = "YouTube Playlist Yönetim Aracı (Pro)"
APP_DIR_NAME = ".yt_playlist_tool"
PREFERENCES_FILE_NAME = "preferences.json"
DEFAULT_LOG_FILE_NAME = "app.log"
DEFAULT_PDF_REPORT_NAME = "pdf_report.txt"
DEFAULT_FAILED_LINKS_NAME = "failed_links.txt"
DEFAULT_ZIP_NAME = "playlist_pdfs.zip"
DEFAULT_JOB_STATE_NAME = "job_state.json"
DEFAULT_HISTORY_FILE_NAME = "history.jsonl"
DEFAULT_TRANSFER_STATE_NAME = "transfer_state.json"
TRANSFER_THROTTLE_SECONDS = 0.0
THROTTLE_MAX_SECONDS = 3.0
DEFAULT_HISTORY_RETENTION_DAYS = 30
ARCHIVE_DIR_NAME = "archive"
DEFAULT_ARCHIVE_MAX_FILES = 20

DEFAULT_HISTORY_RETENTION = timedelta(days=DEFAULT_HISTORY_RETENTION_DAYS)

DEFAULT_TOPIC_KEYWORDS = [
    "limit",
    "turev",
    "integral",
    "logaritma",
    "trigonometri",
    "geometri",
    "fonksiyon",
    "olasilik",
    "istatistik",
    "denklem",
    "polinom",
    "matematik",
    "ayt",
    "tyt",
]


@dataclass(frozen=True)
class Theme:
    """ttk arayüzünde kullanılan koyu tema renk paleti."""

    bg: str = "#1E1E1E"
    panel_bg: str = "#252526"
    fg: str = "#F3F3F3"
    accent: str = "#0E639C"
    accent_active: str = "#1177BB"
    danger: str = "#B33A3A"
    entry_bg: str = "#2D2D30"
    muted: str = "#A9A9A9"


def get_app_dir() -> Path:
    """Uygulama veri klasörünü döndürür, yoksa oluşturur."""
    app_dir = Path.home() / APP_DIR_NAME
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir
