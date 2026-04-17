"""PDF download, validation, and ZIP packaging operations."""

from __future__ import annotations

import json
import logging
import mimetypes
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from yt_playlist_tool.config import (
    DEFAULT_FAILED_LINKS_NAME,
    DEFAULT_JOB_STATE_NAME,
    DEFAULT_PDF_REPORT_NAME,
    DEFAULT_TOPIC_KEYWORDS,
    DEFAULT_ZIP_NAME,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_FACTOR,
    RETRY_TOTAL,
)
from yt_playlist_tool.utils.helpers import ensure_directory, safe_filename
from yt_playlist_tool.utils.parsers import (
    convert_drive_link_to_direct,
    extract_pdf_links_from_text,
    tokenize_for_topic,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoRef:
    """Minimal video model required by the PDF service."""

    video_id: str
    title: str


@dataclass(frozen=True)
class FailedLink:
    """Report record for links that could not be downloaded."""

    video_id: str
    video_title: str
    url: str
    error: str


@dataclass
class PdfDownloadReport:
    """Result model for bulk PDF download and ZIP operations."""

    total_videos: int = 0
    videos_with_links: int = 0
    discovered_links: int = 0
    downloaded_pdfs: int = 0
    zip_path: str = ""
    cancelled: bool = False
    failed_links: list[FailedLink] = field(default_factory=list)
    resumed_from_state: bool = False


class PdfService:
    """Download PDF links from video descriptions and package them in a ZIP."""

    def __init__(
        self,
        *,
        timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        retry_total: int = RETRY_TOTAL,
        retry_backoff_factor: float = RETRY_BACKOFF_FACTOR,
    ) -> None:
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.retry_total = max(0, int(retry_total))
        self.retry_backoff_factor = max(0.1, float(retry_backoff_factor))
        self._build_session()

    def update_retry_policy(self, timeout_seconds: int, retry_total: int, retry_backoff_factor: float) -> None:
        """Update retry settings without recreating the service object."""
        self.timeout_seconds = max(5, int(timeout_seconds))
        self.retry_total = max(0, int(retry_total))
        self.retry_backoff_factor = max(0.1, float(retry_backoff_factor))
        self._build_session()

    def _build_session(self) -> None:
        """Prepare an HTTP session with current retry settings."""
        retry = Retry(
            total=self.retry_total,
            connect=self.retry_total,
            read=self.retry_total,
            backoff_factor=self.retry_backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def process_videos(
        self,
        videos: list[VideoRef],
        descriptions: dict[str, str],
        output_dir: Path,
        cancel_requested: Callable[[], bool],
        progress_cb: Callable[[int, int], None],
        resume_from_state: bool = False,
    ) -> PdfDownloadReport:
        """Download links from descriptions and place files into topic-based ZIP folders."""
        report = PdfDownloadReport(total_videos=len(videos))
        ensure_directory(output_dir)
        temp_dir = ensure_directory(output_dir / "_temp_pdfs")
        downloaded_files: list[Path] = []
        state_path = output_dir / DEFAULT_JOB_STATE_NAME
        done_video_ids: set[str] = set()

        if resume_from_state and state_path.exists():
            done_video_ids = self._load_state(state_path)
            report.resumed_from_state = bool(done_video_ids)

        for video_position, video in enumerate(videos, start=1):
            if cancel_requested():
                report.cancelled = True
                break

            progress_cb(video_position, len(videos))
            if video.video_id in done_video_ids:
                continue
            description = descriptions.get(video.video_id, "")
            links = extract_pdf_links_from_text(description)
            if not links:
                done_video_ids.add(video.video_id)
                self._save_state(state_path, done_video_ids)
                continue

            report.videos_with_links += 1
            report.discovered_links += len(links)

            for link_number, link in enumerate(links, start=1):
                if cancel_requested():
                    report.cancelled = True
                    break

                direct_url = convert_drive_link_to_direct(link)
                file_name = safe_filename(video.title, suffix=f"{link_number}.pdf")
                file_path = temp_dir / file_name

                try:
                    self._download_pdf(direct_url, file_path)
                except Exception as exc:
                    # Continue processing even when a single link fails.
                    report.failed_links.append(
                        FailedLink(
                            video_id=video.video_id,
                            video_title=video.title,
                            url=direct_url,
                            error=str(exc),
                        )
                    )
                    if file_path.exists():
                        file_path.unlink(missing_ok=True)
                    logger.warning("PDF download failed: %s (%s)", direct_url, exc)
                    continue

                downloaded_files.append(file_path)
                report.downloaded_pdfs += 1

            done_video_ids.add(video.video_id)
            self._save_state(state_path, done_video_ids)

        if downloaded_files:
            zip_path = output_dir / DEFAULT_ZIP_NAME
            self._create_zip_with_topic_folders(downloaded_files, zip_path)
            report.zip_path = str(zip_path)

        self._cleanup_files(downloaded_files)
        temp_dir.rmdir() if temp_dir.exists() and not any(temp_dir.iterdir()) else None

        self._write_report_files(output_dir, report)
        if not report.cancelled and state_path.exists():
            state_path.unlink(missing_ok=True)
        return report

    def _download_pdf(self, url: str, destination: Path) -> None:
        """Download one PDF with timeout and content-type validation."""
        response = self.session.get(url, stream=True, timeout=self.timeout_seconds)
        response.raise_for_status()

        # Some servers return octet-stream for PDFs, so extension-based detection is also allowed.
        content_type = (response.headers.get("content-type") or "").lower()
        guessed_type, _ = mimetypes.guess_type(str(destination))
        looks_like_pdf = "pdf" in content_type or guessed_type == "application/pdf"
        if not looks_like_pdf:
            raise ValueError(f"Unexpected content type: {content_type or 'unknown'}")

        with destination.open("wb") as stream:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    stream.write(chunk)

        if destination.stat().st_size == 0:
            raise ValueError("Downloaded file is empty.")

    def _create_zip_with_topic_folders(self, files: list[Path], zip_path: Path) -> None:
        """Put files into the ZIP using deterministic folder selection rules."""
        token_map: dict[Path, list[str]] = {}
        token_counter: Counter[str] = Counter()

        for file_path in sorted(files, key=lambda p: p.name.lower()):
            tokens = tokenize_for_topic(file_path.stem)
            token_map[file_path] = tokens
            token_counter.update(set(tokens))

        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file_path in sorted(files, key=lambda p: p.name.lower()):
                tokens = token_map[file_path]
                folder = self._choose_folder(tokens, token_counter)
                arcname = f"{folder}/{file_path.name}" if folder else file_path.name
                archive.write(file_path, arcname=arcname)

    def _choose_folder(self, tokens: list[str], token_counter: Counter[str]) -> str:
        """Choose folder by topic keywords first, then by common tokens."""
        for keyword in DEFAULT_TOPIC_KEYWORDS:
            if keyword in tokens:
                return keyword

        common_tokens = [token for token in tokens if token_counter[token] >= 2]
        if common_tokens:
            return sorted(common_tokens)[0]
        return "uncategorized"

    @staticmethod
    def _cleanup_files(files: list[Path]) -> None:
        """Delete temporary PDF files after ZIP creation."""
        for file_path in files:
            try:
                file_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Could not remove temporary file: %s", file_path)

    @staticmethod
    def _write_report_files(output_dir: Path, report: PdfDownloadReport) -> None:
        """Write summary and failed-link reports to disk."""
        report_path = output_dir / DEFAULT_PDF_REPORT_NAME
        report_lines = [
            f"Total videos: {report.total_videos}",
            f"Videos with links: {report.videos_with_links}",
            f"Discovered links: {report.discovered_links}",
            f"Downloaded PDFs: {report.downloaded_pdfs}",
            f"Cancelled: {'Yes' if report.cancelled else 'No'}",
            f"ZIP: {report.zip_path or 'Not created'}",
            f"Failed links: {len(report.failed_links)}",
        ]
        report_path.write_text("\n".join(report_lines), encoding="utf-8")

        failed_path = output_dir / DEFAULT_FAILED_LINKS_NAME
        if report.failed_links:
            failed_lines = [
                f"{item.video_id}\t{item.video_title}\t{item.url}\t{item.error}"
                for item in report.failed_links
            ]
        else:
            failed_lines = ["No failed links."]
        failed_path.write_text("\n".join(failed_lines), encoding="utf-8")

    @staticmethod
    def _save_state(state_path: Path, done_video_ids: set[str]) -> None:
        """Write state to support resume-after-interruption behavior."""
        payload = {"done_video_ids": sorted(done_video_ids)}
        state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _load_state(state_path: Path) -> set[str]:
        """Load state left from a previous run."""
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            ids = payload.get("done_video_ids", [])
            return {str(item) for item in ids}
        except (OSError, json.JSONDecodeError):
            return set()
