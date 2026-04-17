"""YouTube API ile konuşan servis katmanı."""

from __future__ import annotations

import logging
import json
import pickle
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from yt_playlist_tool.config import (
    MAX_API_RESULTS,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_FACTOR,
    RETRY_TOTAL,
    THROTTLE_MAX_SECONDS,
    TRANSFER_THROTTLE_SECONDS,
    SCOPES,
    YOUTUBE_API_SERVICE_NAME,
    YOUTUBE_API_VERSION,
)
from yt_playlist_tool.utils.parsers import build_search_terms, title_matches_terms

logger = logging.getLogger(__name__)


class YouTubeServiceError(Exception):
    """YouTube servisinde beklenen hatalar için temel sınıf."""


class AuthError(YouTubeServiceError):
    """OAuth kurulumu veya kimlik doğrulaması başarısız olduğunda fırlatılır."""


@dataclass(frozen=True)
class VideoItem:
    """UI ile servis arasında taşınan sade video modeli."""

    video_id: str
    title: str
    source_playlist_id: str


@dataclass(frozen=True)
class TransferStats:
    """Playlist transferinin özet sonuçları."""

    target_playlist_id: str
    target_created: bool
    requested_count: int
    added_count: int
    skipped_duplicate_count: int
    failed_count: int
    resumed_from_state: bool = False
    cancelled: bool = False


@dataclass(frozen=True)
class RetryPolicy:
    """API çağrıları için retry ve backoff ayarları."""

    timeout_seconds: int = REQUEST_TIMEOUT_SECONDS
    retry_total: int = RETRY_TOTAL
    retry_backoff_factor: float = RETRY_BACKOFF_FACTOR
    transfer_throttle_seconds: float = TRANSFER_THROTTLE_SECONDS


@dataclass(frozen=True)
class RetryEvent:
    """Tek bir retry/backoff olayının kayıt modeli."""

    operation: str
    attempt: int
    max_attempts: int
    status: int | None
    error: str
    delay_seconds: float


class YouTubeService:
    """YouTube API çağrılarının tamamını tek noktada yönetir."""

    def __init__(
        self,
        client_secret_path: Path,
        token_path: Path,
        *,
        timeout_seconds: int = REQUEST_TIMEOUT_SECONDS,
        retry_total: int = RETRY_TOTAL,
        retry_backoff_factor: float = RETRY_BACKOFF_FACTOR,
        transfer_throttle_seconds: float = TRANSFER_THROTTLE_SECONDS,
    ) -> None:
        self.client_secret_path = client_secret_path
        self.token_path = token_path
        self._youtube = None
        self.retry_policy = RetryPolicy(
            timeout_seconds=max(5, int(timeout_seconds)),
            retry_total=max(0, int(retry_total)),
            retry_backoff_factor=max(0.1, float(retry_backoff_factor)),
            transfer_throttle_seconds=max(0.0, float(transfer_throttle_seconds)),
        )
        self._retry_events: list[RetryEvent] = []

    def update_retry_policy(
        self,
        timeout_seconds: int,
        retry_total: int,
        retry_backoff_factor: float,
        transfer_throttle_seconds: float,
    ) -> None:
        """Ayarlar ekranından gelen retry politikasını anında günceller."""
        self.retry_policy = RetryPolicy(
            timeout_seconds=max(5, int(timeout_seconds)),
            retry_total=max(0, int(retry_total)),
            retry_backoff_factor=max(0.1, float(retry_backoff_factor)),
            transfer_throttle_seconds=max(0.0, float(transfer_throttle_seconds)),
        )

    def _execute(self, request_builder: Callable[[], object], operation_name: str) -> dict:
        """API isteğini kontrollü retry/backoff ile çalıştırır."""
        attempts = self.retry_policy.retry_total + 1
        for attempt in range(1, attempts + 1):
            try:
                return request_builder().execute(num_retries=0)
            except HttpError as exc:
                status = getattr(exc.resp, "status", None)
                retryable = status in (429, 500, 502, 503, 504)
                if not retryable or attempt >= attempts:
                    raise
                delay = self.retry_policy.retry_backoff_factor * (2 ** (attempt - 1))
                self._retry_events.append(
                    RetryEvent(
                        operation=operation_name,
                        attempt=attempt,
                        max_attempts=attempts,
                        status=status,
                        error=str(exc),
                        delay_seconds=delay,
                    )
                )
                logger.warning(
                    "%s HTTP %s, retry %s/%s after %.1fs",
                    operation_name,
                    status,
                    attempt,
                    attempts,
                    delay,
                )
                time.sleep(delay)
            except (TimeoutError, OSError) as exc:
                if attempt >= attempts:
                    raise YouTubeServiceError(f"{operation_name} başarısız: {exc}") from exc
                delay = self.retry_policy.retry_backoff_factor * (2 ** (attempt - 1))
                self._retry_events.append(
                    RetryEvent(
                        operation=operation_name,
                        attempt=attempt,
                        max_attempts=attempts,
                        status=None,
                        error=str(exc),
                        delay_seconds=delay,
                    )
                )
                logger.warning(
                    "%s ağ hatası, retry %s/%s after %.1fs",
                    operation_name,
                    attempt,
                    attempts,
                    delay,
                )
                time.sleep(delay)
        raise YouTubeServiceError(f"{operation_name} başarısız.")

    def consume_retry_events(self) -> list[RetryEvent]:
        """Biriken retry olaylarını döndürür ve listeyi sıfırlar."""
        collected_events = list(self._retry_events)
        self._retry_events.clear()
        return collected_events

    def connect(self) -> None:
        """Kimlik doğrulamasını yapıp YouTube istemcisini başlatır."""
        credentials = None
        if self.token_path.exists():
            with self.token_path.open("rb") as token_file:
                credentials = pickle.load(token_file)

        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                if not self.client_secret_path.exists():
                    raise AuthError(f"{self.client_secret_path.name} bulunamadı.")
                flow = InstalledAppFlow.from_client_secrets_file(str(self.client_secret_path), SCOPES)
                credentials = flow.run_local_server(port=0)
            with self.token_path.open("wb") as token_file:
                pickle.dump(credentials, token_file)

        self._youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, credentials=credentials)
        logger.info("YouTube API bağlantısı kuruldu.")

    @property
    def client(self):
        """Hazır durumdaki YouTube istemcisini döndürür."""
        if self._youtube is None:
            raise AuthError("YouTube servisi henüz başlatılmadı.")
        return self._youtube

    def fetch_playlist_items(self, playlist_id: str, search_text: str = "") -> list[VideoItem]:
        """Playlist videolarını çeker, istenirse başlığa göre filtreler."""
        matched_videos: list[VideoItem] = []
        next_page_token = None
        terms = build_search_terms(search_text)

        while True:
            response = self._execute(
                lambda: self.client.playlistItems().list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=MAX_API_RESULTS,
                    pageToken=next_page_token,
                ),
                operation_name=f"playlistItems.list({playlist_id})",
            )

            for item in response.get("items", []):
                video_id = item.get("contentDetails", {}).get("videoId")
                title = item.get("snippet", {}).get("title", "")
                if not video_id:
                    continue
                if title_matches_terms(title, terms):
                    matched_videos.append(
                        VideoItem(video_id=video_id, title=title, source_playlist_id=playlist_id)
                    )

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        return matched_videos

    def fetch_existing_video_ids(self, playlist_id: str) -> set[str]:
        """Hedef playlistteki video kimliklerini duplike kontrolü için döndürür."""
        existing_video_ids: set[str] = set()
        next_page_token = None
        while True:
            try:
                response = self._execute(
                    lambda: self.client.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=MAX_API_RESULTS,
                        pageToken=next_page_token,
                    ),
                    operation_name=f"playlistItems.list(existing:{playlist_id})",
                )
            except HttpError as exc:
                if exc.resp.status == 404:
                    return set()
                raise

            for item in response.get("items", []):
                vid = item.get("contentDetails", {}).get("videoId")
                if vid:
                    existing_video_ids.add(vid)

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break
        return existing_video_ids

    def create_playlist(self, title: str, description: str = "") -> str:
        """Yeni private playlist oluşturup kimliğini döndürür."""
        response = self._execute(
            lambda: self.client.playlists().insert(
                part="snippet,status",
                body={
                    "snippet": {
                        "title": title,
                        "description": description or "Uygulama tarafından otomatik oluşturuldu.",
                    },
                    "status": {"privacyStatus": "private"},
                },
            ),
            operation_name="playlists.insert",
        )
        return response["id"]

    def preview_add_videos(self, video_ids: list[str], target_playlist_id: str | None) -> TransferStats:
        """Gerçek ekleme yapmadan transfer sonucunu hesaplar."""
        seen_video_ids: set[str] = set()
        unique_ids: list[str] = []
        for video_id in video_ids:
            if video_id not in seen_video_ids:
                seen_video_ids.add(video_id)
                unique_ids.append(video_id)

        if not target_playlist_id:
            return TransferStats(
                target_playlist_id="(yeni playlist oluşturulacak)",
                target_created=True,
                requested_count=len(video_ids),
                added_count=len(unique_ids),
                skipped_duplicate_count=len(video_ids) - len(unique_ids),
                failed_count=0,
            )

        existing = self.fetch_existing_video_ids(target_playlist_id)
        would_add = 0
        skipped = 0
        for video_id in unique_ids:
            if video_id in existing:
                skipped += 1
            else:
                would_add += 1

        return TransferStats(
            target_playlist_id=target_playlist_id,
            target_created=False,
            requested_count=len(video_ids),
            added_count=would_add,
            skipped_duplicate_count=skipped + (len(video_ids) - len(unique_ids)),
            failed_count=0,
        )

    def add_videos_to_playlist(
        self,
        video_ids: list[str],
        target_playlist_id: str | None,
        target_playlist_name: str,
        cancel_requested: Callable[[], bool],
        progress_cb: Callable[[int, int], None],
        resume_from_state: bool = False,
        state_path: Path | None = None,
    ) -> TransferStats:
        """Videoları hedef playlist'e ekler; duplike kontrolü ve iptali destekler."""
        target_created = False
        failures = 0
        skipped_duplicates = 0
        added = 0
        requested = len(video_ids)
        cancelled = False
        resumed = False
        processed_video_ids: set[str] = set()

        if resume_from_state and state_path and state_path.exists():
            state_data = self._load_transfer_state(state_path)
            state_target = state_data.get("target_playlist_id")
            if state_target:
                target_playlist_id = state_target
            processed_video_ids = set(state_data.get("processed_video_ids", []))
            added = int(state_data.get("added_count", 0))
            skipped_duplicates = int(state_data.get("skipped_duplicate_count", 0))
            failures = int(state_data.get("failed_count", 0))
            target_created = bool(state_data.get("target_created", False))
            resumed = True

        if not target_playlist_id:
            title = target_playlist_name.strip() or "Python Otomatik Alt Liste"
            target_playlist_id = self.create_playlist(title=title)
            target_created = True
            logger.info("Yeni hedef playlist oluşturuldu: %s", target_playlist_id)
            if state_path:
                self._save_transfer_state(
                    state_path=state_path,
                    target_playlist_id=target_playlist_id,
                    target_created=target_created,
                    processed_video_ids=processed_video_ids,
                    added_count=added,
                    skipped_duplicate_count=skipped_duplicates,
                    failed_count=failures,
                )

        existing = self.fetch_existing_video_ids(target_playlist_id)
        dynamic_throttle = self.retry_policy.transfer_throttle_seconds

        for position, video_id in enumerate(video_ids, start=1):
            if cancel_requested():
                logger.warning("Transfer user tarafından iptal edildi.")
                cancelled = True
                break

            progress_cb(position, requested)
            if video_id in processed_video_ids:
                continue

            if video_id in existing:
                skipped_duplicates += 1
                processed_video_ids.add(video_id)
                if state_path:
                    self._save_transfer_state(
                        state_path=state_path,
                        target_playlist_id=target_playlist_id,
                        target_created=target_created,
                        processed_video_ids=processed_video_ids,
                        added_count=added,
                        skipped_duplicate_count=skipped_duplicates,
                        failed_count=failures,
                    )
                continue

            if dynamic_throttle > 0:
                time.sleep(dynamic_throttle)
            try:
                self._execute(
                    lambda: self.client.playlistItems().insert(
                        part="snippet",
                        body={
                            "snippet": {
                                "playlistId": target_playlist_id,
                                "resourceId": {"kind": "youtube#video", "videoId": video_id},
                            }
                        },
                    ),
                    operation_name=f"playlistItems.insert({video_id})",
                )
            except HttpError:
                failures += 1
                logger.exception("Video hedef playlist'e eklenemedi: %s", video_id)
                dynamic_throttle = min(THROTTLE_MAX_SECONDS, max(dynamic_throttle, 0.1) * 1.5)
                processed_video_ids.add(video_id)
                if state_path:
                    self._save_transfer_state(
                        state_path=state_path,
                        target_playlist_id=target_playlist_id,
                        target_created=target_created,
                        processed_video_ids=processed_video_ids,
                        added_count=added,
                        skipped_duplicate_count=skipped_duplicates,
                        failed_count=failures,
                    )
                continue

            existing.add(video_id)
            added += 1
            processed_video_ids.add(video_id)
            if dynamic_throttle > self.retry_policy.transfer_throttle_seconds:
                dynamic_throttle = max(
                    self.retry_policy.transfer_throttle_seconds,
                    dynamic_throttle * 0.9,
                )
            if state_path:
                self._save_transfer_state(
                    state_path=state_path,
                    target_playlist_id=target_playlist_id,
                    target_created=target_created,
                    processed_video_ids=processed_video_ids,
                    added_count=added,
                    skipped_duplicate_count=skipped_duplicates,
                    failed_count=failures,
                )

        if not cancelled and state_path and state_path.exists():
            state_path.unlink(missing_ok=True)

        return TransferStats(
            target_playlist_id=target_playlist_id,
            target_created=target_created,
            requested_count=requested,
            added_count=added,
            skipped_duplicate_count=skipped_duplicates,
            failed_count=failures,
            resumed_from_state=resumed,
            cancelled=cancelled,
        )

    def fetch_video_descriptions(
        self,
        video_ids: list[str],
        cancel_requested: Callable[[], bool],
        progress_cb: Callable[[int, int], None],
    ) -> dict[str, str]:
        """Video açıklamalarını `video_id -> description` haritası olarak döndürür."""
        descriptions: dict[str, str] = {}
        unique_ids = list(dict.fromkeys(video_ids))
        total = len(unique_ids)
        processed = 0

        for batch_offset in range(0, len(unique_ids), MAX_API_RESULTS):
            if cancel_requested():
                logger.warning("Açıklama çekme işlemi iptal edildi.")
                break

            batch = unique_ids[batch_offset : batch_offset + MAX_API_RESULTS]
            progress_cb(processed, total)
            try:
                response = self._execute(
                    lambda b=",".join(batch): self.client.videos().list(part="snippet", id=b),
                    operation_name=f"videos.list(batch:{batch_offset // MAX_API_RESULTS + 1})",
                )
                items = response.get("items", [])
                if not items:
                    processed += len(batch)
                    progress_cb(processed, total)
                    continue

                for item in items:
                    video_id = item.get("id")
                    if not video_id:
                        continue
                    descriptions[video_id] = item.get("snippet", {}).get("description", "")
            except HttpError:
                logger.exception("Video açıklama batch alınamadı.")
            finally:
                processed += len(batch)
                progress_cb(processed, total)

        return descriptions

    @staticmethod
    def _save_transfer_state(
        *,
        state_path: Path,
        target_playlist_id: str,
        target_created: bool,
        processed_video_ids: set[str],
        added_count: int,
        skipped_duplicate_count: int,
        failed_count: int,
    ) -> None:
        """Transfer state bilgisini diske yazar."""
        payload = {
            "target_playlist_id": target_playlist_id,
            "target_created": target_created,
            "processed_video_ids": sorted(processed_video_ids),
            "added_count": added_count,
            "skipped_duplicate_count": skipped_duplicate_count,
            "failed_count": failed_count,
        }
        state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _load_transfer_state(state_path: Path) -> dict:
        """Daha önce kaydedilmiş transfer state bilgisini okur."""
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
