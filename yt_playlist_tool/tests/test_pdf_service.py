"""Unit tests for PDF service deterministic behavior."""

from __future__ import annotations

import zipfile
from collections import Counter
from pathlib import Path

from yt_playlist_tool.services.pdf_service import PdfService


def test_choose_folder_prefers_topic_keyword() -> None:
    service = PdfService()
    folder = service._choose_folder(["matematik", "kamp"], Counter({"kamp": 3, "matematik": 1}))
    assert folder == "matematik"


def test_choose_folder_falls_back_to_common_token() -> None:
    service = PdfService()
    folder = service._choose_folder(["kamp", "hafta1"], Counter({"kamp": 2, "hafta1": 1}))
    assert folder == "kamp"


def test_create_zip_with_topic_folders_is_deterministic(tmp_path: Path) -> None:
    service = PdfService()
    files = []
    names = ["limit_notlari_1.pdf", "limit_notlari_2.pdf", "geometri_ozet.pdf"]

    for name in names:
        file_path = tmp_path / name
        file_path.write_bytes(b"dummy")
        files.append(file_path)

    zip_path = tmp_path / "out.zip"
    service._create_zip_with_topic_folders(files, zip_path)

    with zipfile.ZipFile(zip_path, "r") as archive:
        namelist = sorted(archive.namelist())

    assert "limit/limit_notlari_1.pdf" in namelist
    assert "limit/limit_notlari_2.pdf" in namelist
    assert "geometri/geometri_ozet.pdf" in namelist
