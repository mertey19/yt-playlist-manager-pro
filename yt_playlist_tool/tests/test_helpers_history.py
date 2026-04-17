"""Tests for history/archive/state helper utilities."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from yt_playlist_tool.utils import helpers


def test_rotate_history_removes_old_entries(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(helpers, "get_app_dir", lambda: tmp_path)
    history_path = tmp_path / "history.jsonl"

    recent = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "event": "recent",
        "payload": {},
    }
    old = {
        "timestamp": (datetime.now() - timedelta(days=45)).isoformat(timespec="seconds"),
        "event": "old",
        "payload": {},
    }
    history_path.write_text(
        json.dumps(old, ensure_ascii=False) + "\n" + json.dumps(recent, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    kept, removed = helpers.rotate_history(days=30)
    assert kept == 1
    assert removed == 1
    lines = history_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["event"] == "recent"


def test_archive_history_moves_content(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(helpers, "get_app_dir", lambda: tmp_path)
    history_path = tmp_path / "history.jsonl"
    history_path.write_text('{"event":"x"}\n', encoding="utf-8")

    archive_path = helpers.archive_history()
    assert archive_path is not None
    assert archive_path.exists()
    assert history_path.read_text(encoding="utf-8") == ""


def test_clear_runtime_state_files_removes_known_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(helpers, "get_app_dir", lambda: tmp_path)
    transfer_state = tmp_path / "transfer_state.json"
    transfer_state.write_text("{}", encoding="utf-8")
    extra = tmp_path / "job_state.json"
    extra.write_text("{}", encoding="utf-8")

    removed = helpers.clear_runtime_state_files(extra_paths=[extra])
    removed_set = {p.name for p in removed}
    assert "transfer_state.json" in removed_set
    assert "job_state.json" in removed_set


def test_should_weekly_archive_true_for_old_timestamp() -> None:
    old = (datetime.now() - timedelta(days=8)).isoformat(timespec="seconds")
    assert helpers.should_weekly_archive(old) is True


def test_archive_history_if_oversize(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(helpers, "get_app_dir", lambda: tmp_path)
    history_path = tmp_path / "history.jsonl"
    history_path.write_text("x" * 250_000, encoding="utf-8")

    archive_path = helpers.archive_history_if_oversize(max_size_mb=0.1)
    assert archive_path is not None
    assert archive_path.exists()


def test_prune_old_archives_keeps_latest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(helpers, "get_app_dir", lambda: tmp_path)
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    for idx in range(5):
        file_path = archive_dir / f"history_2026010{idx}_000000.jsonl"
        file_path.write_text(f"{idx}", encoding="utf-8")

    kept, removed = helpers.prune_old_archives(max_files=2)
    assert kept == 2
    assert removed == 3
    assert len(list(archive_dir.glob("history_*.jsonl"))) == 2
