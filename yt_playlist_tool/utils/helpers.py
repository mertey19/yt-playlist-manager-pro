"""Shared helper functions used by service and UI layers."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from yt_playlist_tool.config import (
    ARCHIVE_DIR_NAME,
    DEFAULT_ARCHIVE_MAX_FILES,
    DEFAULT_HISTORY_RETENTION_DAYS,
    DEFAULT_JOB_STATE_NAME,
    DEFAULT_HISTORY_FILE_NAME,
    DEFAULT_LOG_FILE_NAME,
    DEFAULT_TRANSFER_STATE_NAME,
    LOG_FORMAT,
    PREFERENCES_FILE_NAME,
    get_app_dir,
)


def normalize_text(text: str) -> str:
    """Normalize Turkish characters and lowercase the text."""
    mapping = str.maketrans(
        {
            "ç": "c",
            "Ç": "c",
            "ğ": "g",
            "Ğ": "g",
            "ı": "i",
            "İ": "i",
            "ö": "o",
            "Ö": "o",
            "ş": "s",
            "Ş": "s",
            "ü": "u",
            "Ü": "u",
        }
    )
    return text.translate(mapping).lower()


def safe_filename(base_name: str, suffix: str = "", max_length: int = 120) -> str:
    """Build a filesystem-safe filename with a controlled length."""
    sanitized = re.sub(r"[^\w\-. ]+", "_", base_name, flags=re.UNICODE)
    sanitized = re.sub(r"\s+", "_", sanitized).strip("._")
    sanitized = sanitized or "file"
    if suffix:
        sanitized = f"{sanitized}_{suffix}"
    return sanitized[:max_length].rstrip("._") or "file"


def ensure_directory(path: Path) -> Path:
    """Create the directory if it does not exist and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def setup_logging() -> Path:
    """Set up app logging and return the log file path."""
    app_dir = get_app_dir()
    log_path = app_dir / DEFAULT_LOG_FILE_NAME

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format=LOG_FORMAT,
            handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
        )
    return log_path


@dataclass
class Preferences:
    """User preferences persisted across sessions."""

    source_playlists_text: str = ""
    target_playlist: str = ""
    target_playlist_name: str = ""
    title_filter: str = ""
    range_text: str = ""
    last_download_dir: str = ""
    request_timeout_seconds: int = 25
    retry_total: int = 3
    retry_backoff_factor: float = 0.7
    transfer_dry_run: bool = False
    transfer_throttle_seconds: float = 0.0
    startup_housekeeping_enabled: bool = True
    history_retention_days: int = 30
    weekly_auto_archive_enabled: bool = True
    history_max_size_mb: float = 10.0
    archive_max_files: int = DEFAULT_ARCHIVE_MAX_FILES
    last_history_archive_at: str = ""


def load_preferences() -> Preferences:
    """Load preferences from JSON, or return defaults on failure."""
    pref_path = get_app_dir() / PREFERENCES_FILE_NAME
    if not pref_path.exists():
        return Preferences()

    try:
        raw = json.loads(pref_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logging.getLogger(__name__).warning("Preferences could not be parsed, defaults used.")
        return Preferences()

    merged_data: dict[str, Any] = {k: raw.get(k, v) for k, v in asdict(Preferences()).items()}
    return Preferences(**merged_data)


def save_preferences(prefs: Preferences) -> None:
    """Save preferences in JSON format."""
    pref_path = get_app_dir() / PREFERENCES_FILE_NAME
    pref_path.write_text(json.dumps(asdict(prefs), ensure_ascii=False, indent=2), encoding="utf-8")


def append_history(event: str, payload: dict[str, Any]) -> None:
    """Append a structured one-line event record to history."""
    history_path = get_app_dir() / DEFAULT_HISTORY_FILE_NAME
    record = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "event": event,
        "payload": payload,
    }
    with history_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_history(limit: int = 50) -> list[dict[str, Any]]:
    """Return recent history records from newest to oldest."""
    history_path = get_app_dir() / DEFAULT_HISTORY_FILE_NAME
    if not history_path.exists():
        return []
    lines = history_path.read_text(encoding="utf-8").splitlines()
    parsed_records: list[dict[str, Any]] = []
    for line in reversed(lines[-limit:]):
        try:
            parsed_records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return parsed_records


def rotate_history(days: int = DEFAULT_HISTORY_RETENTION_DAYS) -> tuple[int, int]:
    """Remove history records older than the configured day window."""
    history_path = get_app_dir() / DEFAULT_HISTORY_FILE_NAME
    if not history_path.exists():
        return (0, 0)

    cutoff = datetime.now() - timedelta(days=max(1, int(days)))
    kept: list[str] = []
    removed = 0
    for line in history_path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
            ts = datetime.fromisoformat(record.get("timestamp", ""))
        except (json.JSONDecodeError, ValueError, TypeError):
            removed += 1
            continue
        if ts >= cutoff:
            kept.append(line)
        else:
            removed += 1

    history_path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
    return (len(kept), removed)


def archive_history() -> Path | None:
    """Move history to a timestamped archive and clear active history."""
    app_dir = get_app_dir()
    history_path = app_dir / DEFAULT_HISTORY_FILE_NAME
    if not history_path.exists() or history_path.stat().st_size == 0:
        return None

    archive_dir = app_dir / ARCHIVE_DIR_NAME
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_path = archive_dir / f"history_{stamp}.jsonl"
    archive_path.write_text(history_path.read_text(encoding="utf-8"), encoding="utf-8")
    history_path.write_text("", encoding="utf-8")
    return archive_path


def get_archive_dir() -> Path:
    """Return the archive directory, creating it if needed."""
    archive_dir = get_app_dir() / ARCHIVE_DIR_NAME
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir


def list_archive_files() -> list[Path]:
    """List archive files with newest entries first."""
    archive_dir = get_archive_dir()
    files = [p for p in archive_dir.glob("history_*.jsonl") if p.is_file()]
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def prune_old_archives(max_files: int) -> tuple[int, int]:
    """Keep only the newest N archives and delete the rest."""
    keep = max(1, int(max_files))
    archives = list_archive_files()
    if len(archives) <= keep:
        return (len(archives), 0)

    removed = 0
    for path in archives[keep:]:
        path.unlink(missing_ok=True)
        removed += 1
    return (keep, removed)


def get_history_file_size_mb() -> float:
    """Return the active history file size in megabytes."""
    history_path = get_app_dir() / DEFAULT_HISTORY_FILE_NAME
    if not history_path.exists():
        return 0.0
    return history_path.stat().st_size / (1024 * 1024)


def should_weekly_archive(last_archive_iso: str) -> bool:
    """Return True when weekly archiving is due."""
    if not last_archive_iso:
        return True
    try:
        last_archive = datetime.fromisoformat(last_archive_iso)
    except ValueError:
        return True
    return datetime.now() - last_archive >= timedelta(days=7)


def archive_history_if_oversize(max_size_mb: float) -> Path | None:
    """Archive history when it exceeds the configured size limit."""
    if get_history_file_size_mb() < max(0.1, float(max_size_mb)):
        return None
    return archive_history()


def clear_state_files(extra_paths: list[Path] | None = None) -> list[Path]:
    """Delete known state files and return removed paths."""
    app_dir = get_app_dir()
    candidates = [
        app_dir / DEFAULT_TRANSFER_STATE_NAME,
        app_dir / DEFAULT_JOB_STATE_NAME,
    ]
    if extra_paths:
        candidates.extend(extra_paths)

    removed: list[Path] = []
    for path in candidates:
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def clear_runtime_state_files(extra_paths: list[Path] | None = None) -> list[Path]:
    """Backward-compatible wrapper for `clear_state_files`."""
    return clear_state_files(extra_paths=extra_paths)
