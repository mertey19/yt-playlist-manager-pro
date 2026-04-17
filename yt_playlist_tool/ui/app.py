"""Main ttk-based desktop application UI."""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Callable

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from yt_playlist_tool.config import (
    APP_TITLE,
    DEFAULT_JOB_STATE_NAME,
    DEFAULT_TRANSFER_STATE_NAME,
    Theme,
    get_app_dir,
)
from yt_playlist_tool.services.pdf_service import PdfService
from yt_playlist_tool.services.youtube_service import (
    AuthError,
    RetryEvent,
    TransferStats,
    VideoItem,
    YouTubeService,
    YouTubeServiceError,
)
from yt_playlist_tool.utils.helpers import (
    Preferences,
    archive_history,
    archive_history_if_oversize,
    append_history,
    clear_runtime_state_files,
    get_history_file_size_mb,
    get_archive_dir,
    list_archive_files,
    load_history,
    load_preferences,
    prune_old_archives,
    rotate_history,
    save_preferences,
    should_weekly_archive,
    setup_logging,
)
from yt_playlist_tool.utils.parsers import (
    build_search_terms,
    extract_playlist_id,
    parse_playlist_id_list,
    parse_range_string,
    title_matches_terms,
)

logger = logging.getLogger(__name__)


class PlaylistApp:
    """Controller + UI composition for the playlist management tool."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1140x760")
        self.theme = Theme()
        self.root.configure(bg=self.theme.bg)

        self.log_file_path = setup_logging()
        self.prefs = load_preferences()

        self.youtube_service = YouTubeService(
            client_secret_path=Path("client_secret.json"),
            token_path=Path("token.pickle"),
            timeout_seconds=self.prefs.request_timeout_seconds,
            retry_total=self.prefs.retry_total,
            retry_backoff_factor=self.prefs.retry_backoff_factor,
            transfer_throttle_seconds=self.prefs.transfer_throttle_seconds,
        )
        self.pdf_service = PdfService(
            timeout_seconds=self.prefs.request_timeout_seconds,
            retry_total=self.prefs.retry_total,
            retry_backoff_factor=self.prefs.retry_backoff_factor,
        )

        self.worker_thread: threading.Thread | None = None
        self.cancel_event = threading.Event()
        self.ui_queue: Queue[Callable[[], None]] = Queue()
        self.youtube_lock = threading.Lock()
        self.youtube_connected = False
        self.last_retry_events: list[RetryEvent] = []
        self.last_housekeeping_report: dict[str, str] = {"status": "Henüz çalışmadı."}

        self.all_videos: list[VideoItem] = []
        self.visible_videos: list[VideoItem] = []

        self.status_var = tk.StringVar(value="Hazır")
        self.stats_var = tk.StringVar(value="Toplam: 0 | Görünen: 0 | Seçili: 0")
        self.dry_run_var = tk.BooleanVar(value=self.prefs.transfer_dry_run)

        self._configure_styles()
        self._build_ui()
        self._apply_preferences()
        self._schedule_ui_queue_poll()
        self._run_startup_housekeeping()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _configure_styles(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background=self.theme.bg)
        style.configure("Panel.TFrame", background=self.theme.panel_bg)
        style.configure("TLabel", background=self.theme.bg, foreground=self.theme.fg)
        style.configure("Panel.TLabel", background=self.theme.panel_bg, foreground=self.theme.fg)
        style.configure(
            "TButton",
            background=self.theme.accent,
            foreground=self.theme.fg,
            borderwidth=0,
            padding=6,
        )
        style.map(
            "TButton",
            background=[("active", self.theme.accent_active), ("disabled", self.theme.entry_bg)],
            foreground=[("disabled", self.theme.muted)],
        )
        style.configure("Danger.TButton", background=self.theme.danger, foreground=self.theme.fg)
        style.map(
            "Danger.TButton",
            background=[("active", "#CC4C4C"), ("disabled", self.theme.entry_bg)],
            foreground=[("disabled", self.theme.muted)],
        )
        style.configure("TEntry", fieldbackground=self.theme.entry_bg, foreground=self.theme.fg)
        style.configure("TProgressbar", troughcolor=self.theme.panel_bg, background=self.theme.accent)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root)
        main.pack(fill="both", expand=True, padx=10, pady=10)

        top = ttk.Frame(main, style="Panel.TFrame", padding=10)
        top.pack(fill="x", pady=(0, 8))

        ttk.Label(
            top,
            text="Kaynak Playlist ID/URL (satır satır veya virgülle):",
            style="Panel.TLabel",
        ).grid(row=0, column=0, sticky="nw", padx=(0, 8), pady=4)
        self.src_text = tk.Text(
            top,
            width=60,
            height=4,
            bg=self.theme.entry_bg,
            fg=self.theme.fg,
            insertbackground=self.theme.fg,
            relief="flat",
        )
        self.src_text.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(top, text="Hedef Playlist ID (boşsa yeni):", style="Panel.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.target_entry = ttk.Entry(top, width=60)
        self.target_entry.grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(top, text="Yeni Playlist Adı:", style="Panel.TLabel").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.target_name_entry = ttk.Entry(top, width=60)
        self.target_name_entry.grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(top, text="Başlık filtresi:", style="Panel.TLabel").grid(
            row=3, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.search_entry = ttk.Entry(top, width=60)
        self.search_entry.grid(row=3, column=1, sticky="ew", pady=4)
        self.search_entry.bind("<KeyRelease>", lambda _: self._refresh_visible_videos())

        ttk.Label(top, text="Aralık (örn: 1-5, 10, 15-20):", style="Panel.TLabel").grid(
            row=4, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.range_entry = ttk.Entry(top, width=60)
        self.range_entry.grid(row=4, column=1, sticky="ew", pady=4)

        top.columnconfigure(1, weight=1)

        buttons = ttk.Frame(main, style="Panel.TFrame", padding=8)
        buttons.pack(fill="x", pady=(0, 8))

        self.fetch_button = ttk.Button(
            buttons, text="Playlist Videolarını Getir", command=self.fetch_videos
        )
        self.fetch_button.pack(side="left", padx=(0, 6))
        self.transfer_button = ttk.Button(
            buttons, text="Seçilen/Aralık Videoları Ekle", command=self.transfer_selected
        )
        self.transfer_button.pack(side="left", padx=6)
        self.pdf_button = ttk.Button(
            buttons, text="PDF İndir + ZIP Oluştur", command=self.download_pdfs
        )
        self.pdf_button.pack(side="left", padx=6)
        ttk.Checkbutton(
            buttons,
            text="Dry-run transfer",
            variable=self.dry_run_var,
            style="Panel.TLabel",
        ).pack(side="left", padx=(10, 0))
        self.cancel_button = ttk.Button(
            buttons, text="İptal", command=self.cancel_current_task, style="Danger.TButton"
        )
        self.cancel_button.pack(side="left", padx=6)
        self.cancel_button.configure(state="disabled")
        ttk.Button(buttons, text="Ayarlar", command=self.open_settings_dialog).pack(
            side="left", padx=6
        )

        self.select_all_button = ttk.Button(buttons, text="Tümünü Seç", command=self.select_all_visible)
        self.select_all_button.pack(side="right", padx=6)
        self.clear_selection_button = ttk.Button(
            buttons, text="Seçimi Temizle", command=self.clear_selection
        )
        self.clear_selection_button.pack(side="right", padx=6)

        list_panel = ttk.Frame(main, style="Panel.TFrame", padding=8)
        list_panel.pack(fill="both", expand=True, pady=(0, 8))
        ttk.Label(list_panel, text="Videolar:", style="Panel.TLabel").pack(anchor="w")

        list_container = ttk.Frame(list_panel, style="Panel.TFrame")
        list_container.pack(fill="both", expand=True)
        self.listbox = tk.Listbox(
            list_container,
            selectmode=tk.EXTENDED,
            bg=self.theme.entry_bg,
            fg=self.theme.fg,
            selectbackground=self.theme.accent,
            selectforeground=self.theme.fg,
            relief="flat",
        )
        self.listbox.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(list_container, orient="vertical", command=self.listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.listbox.configure(yscrollcommand=scrollbar.set)
        self.listbox.bind("<<ListboxSelect>>", lambda _: self._update_stats())

        self.menu = tk.Menu(self.root, tearoff=0)
        self.menu.add_command(label="Tümünü Seç", command=self.select_all_visible)
        self.menu.add_command(label="Seçimi Temizle", command=self.clear_selection)
        self.listbox.bind("<Button-3>", self._show_context_menu)

        bottom = ttk.Frame(main, style="Panel.TFrame", padding=8)
        bottom.pack(fill="x", pady=(0, 8))

        self.progress = ttk.Progressbar(bottom, mode="determinate")
        self.progress.pack(fill="x", pady=(0, 6))
        ttk.Label(bottom, textvariable=self.stats_var, style="Panel.TLabel").pack(anchor="w")

        action_bar = ttk.Frame(bottom, style="Panel.TFrame")
        action_bar.pack(fill="x", pady=(6, 0))
        ttk.Button(action_bar, text="Log'u TXT Olarak Dışa Aktar", command=self.export_log).pack(
            side="left", padx=(0, 6)
        )
        ttk.Button(action_bar, text="Listeyi CSV Dışa Aktar", command=self.export_videos_csv).pack(
            side="left"
        )
        ttk.Button(action_bar, text="İşlem Geçmişi", command=self.show_history_dialog).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(action_bar, text="Retry Detayları", command=self.show_retry_details_dialog).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(action_bar, text="Maintenance", command=self.show_maintenance_dialog).pack(
            side="left", padx=(6, 0)
        )

        log_panel = ttk.Frame(main, style="Panel.TFrame", padding=8)
        log_panel.pack(fill="both", expand=False)
        ttk.Label(log_panel, text="Log:", style="Panel.TLabel").pack(anchor="w")
        self.log_text = tk.Text(
            log_panel,
            height=10,
            bg=self.theme.entry_bg,
            fg=self.theme.fg,
            wrap="word",
            relief="flat",
        )
        self.log_text.pack(fill="both", expand=True)

        status = ttk.Frame(self.root, style="Panel.TFrame", padding=(8, 4))
        status.pack(fill="x")
        ttk.Label(status, textvariable=self.status_var, style="Panel.TLabel").pack(anchor="w")

    def _show_context_menu(self, event: tk.Event) -> None:
        try:
            self.menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.menu.grab_release()

    def _apply_preferences(self) -> None:
        self.src_text.insert("1.0", self.prefs.source_playlists_text)
        self.target_entry.insert(0, self.prefs.target_playlist)
        self.target_name_entry.insert(0, self.prefs.target_playlist_name)
        self.search_entry.insert(0, self.prefs.title_filter)
        self.range_entry.insert(0, self.prefs.range_text)

    def _run_startup_housekeeping(self) -> None:
        """Perform automatic maintenance tasks at startup."""
        if not self.prefs.startup_housekeeping_enabled:
            self.last_housekeeping_report = {"status": "Startup housekeeping kapalı."}
            return
        def worker() -> None:
            kept, removed = rotate_history(days=self.prefs.history_retention_days)
            size_before = get_history_file_size_mb()
            archived_by_size = archive_history_if_oversize(self.prefs.history_max_size_mb)
            archived_weekly = None
            if self.prefs.weekly_auto_archive_enabled and should_weekly_archive(
                self.prefs.last_history_archive_at
            ):
                archived_weekly = archive_history()

            archive_path = archived_by_size or archived_weekly
            archives_kept, archives_removed = prune_old_archives(self.prefs.archive_max_files)
            if archive_path is not None:
                self.prefs.last_history_archive_at = datetime.now().isoformat(timespec="seconds")
                save_preferences(self.prefs)

            report = {
                "retention_days": str(self.prefs.history_retention_days),
                "kept": str(kept),
                "removed": str(removed),
                "size_before_mb": f"{size_before:.2f}",
                "archive_created": "Evet" if archive_path else "Hayır",
                "archive_path": str(archive_path) if archive_path else "-",
                "archives_kept": str(archives_kept),
                "archives_removed": str(archives_removed),
                "archive_max_files": str(self.prefs.archive_max_files),
            }

            def finish() -> None:
                self.last_housekeeping_report = report
                append_history("startup_housekeeping", report)
                if removed > 0 or archive_path is not None or archives_removed > 0:
                    self._append_log(
                        (
                            "Startup housekeeping çalıştı: "
                            f"removed={removed}, archive={'evet' if archive_path else 'hayır'}, "
                            f"archive_pruned={archives_removed}"
                        )
                    )

            self._ui_call(finish)

        threading.Thread(target=worker, daemon=True).start()

    def _current_preferences(self) -> Preferences:
        return Preferences(
            source_playlists_text=self.src_text.get("1.0", tk.END).strip(),
            target_playlist=self.target_entry.get().strip(),
            target_playlist_name=self.target_name_entry.get().strip(),
            title_filter=self.search_entry.get().strip(),
            range_text=self.range_entry.get().strip(),
            last_download_dir=self.prefs.last_download_dir,
            request_timeout_seconds=self.prefs.request_timeout_seconds,
            retry_total=self.prefs.retry_total,
            retry_backoff_factor=self.prefs.retry_backoff_factor,
            transfer_dry_run=self.dry_run_var.get(),
            transfer_throttle_seconds=self.prefs.transfer_throttle_seconds,
            startup_housekeeping_enabled=self.prefs.startup_housekeeping_enabled,
            history_retention_days=self.prefs.history_retention_days,
            weekly_auto_archive_enabled=self.prefs.weekly_auto_archive_enabled,
            history_max_size_mb=self.prefs.history_max_size_mb,
            archive_max_files=self.prefs.archive_max_files,
            last_history_archive_at=self.prefs.last_history_archive_at,
        )

    def _on_close(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            if not messagebox.askyesno("Çıkış", "Çalışan işlem var. Çıkmak için iptal edilsin mi?"):
                return
            self.cancel_event.set()
        save_preferences(self._current_preferences())
        self.root.destroy()

    def _append_log(self, text: str) -> None:
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        logger.info(text)

    def _set_status(self, text: str) -> None:
        self.status_var.set(text)

    def _set_progress(self, current: int, total: int) -> None:
        if total <= 0:
            self.progress.configure(value=0, maximum=1)
            return
        self.progress.configure(maximum=total, value=current)

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        for btn in (self.fetch_button, self.transfer_button, self.pdf_button):
            btn.configure(state=state)
        self.cancel_button.configure(state="normal" if busy else "disabled")

    def _schedule_ui_queue_poll(self) -> None:
        self.root.after(100, self._process_ui_queue)

    def _process_ui_queue(self) -> None:
        while True:
            try:
                callback = self.ui_queue.get_nowait()
            except Empty:
                break
            callback()
        self._schedule_ui_queue_poll()

    def _ui_call(self, callback: Callable[[], None]) -> None:
        self.ui_queue.put(callback)

    def _ensure_connected(self) -> bool:
        if self.youtube_connected:
            return True
        with self.youtube_lock:
            if self.youtube_connected:
                return True
            try:
                self.youtube_service.connect()
            except (AuthError, YouTubeServiceError, OSError) as exc:
                self._ui_call(lambda: messagebox.showerror("YouTube Bağlantı Hatası", str(exc)))
                self._ui_call(lambda: self._append_log(f"HATA: Bağlantı kurulamadı: {exc}"))
                return False
            self.youtube_connected = True
            self._ui_call(lambda: self._append_log("YouTube servisine bağlanıldı."))
            return True

    def _run_async(self, task_name: str, worker: Callable[[], None]) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Meşgul", "Önce mevcut işlemi tamamlayın veya iptal edin.")
            return

        self.cancel_event.clear()
        self._set_busy(True)
        self._set_status(f"{task_name} çalışıyor...")

        def wrapper() -> None:
            try:
                worker()
            finally:
                retry_events = self.youtube_service.consume_retry_events()
                self.last_retry_events = retry_events
                if retry_events:
                    self._ui_call(
                        lambda n=len(retry_events): self._append_log(
                            f"Retry olayı kaydedildi: {n} adet (Retry Detayları ile inceleyebilirsiniz)."
                        )
                    )
                self._ui_call(lambda: self._set_busy(False))
                self._ui_call(lambda: self._set_status("Hazır"))
                self._ui_call(lambda: self._set_progress(0, 0))

        self.worker_thread = threading.Thread(target=wrapper, daemon=True)
        self.worker_thread.start()

    def cancel_current_task(self) -> None:
        self.cancel_event.set()
        self._set_status("İptal talebi gönderildi...")
        self._append_log("İptal talebi alındı, işlem güvenli noktada sonlandırılacak.")

    def fetch_videos(self) -> None:
        playlist_ids = parse_playlist_id_list(self.src_text.get("1.0", tk.END))
        if not playlist_ids:
            messagebox.showerror("Hatalı Girdi", "En az bir geçerli kaynak playlist giriniz.")
            return

        api_filter = self.search_entry.get().strip()

        def worker() -> None:
            if not self._ensure_connected():
                return

            seen_ids: set[str] = set()
            merged: list[VideoItem] = []
            duplicate_skipped = 0
            total_raw = 0

            for idx, pid in enumerate(playlist_ids, start=1):
                if self.cancel_event.is_set():
                    self._ui_call(lambda: self._append_log("Video çekme işlemi iptal edildi."))
                    return
                self._ui_call(lambda i=idx: self._set_progress(i, len(playlist_ids)))
                self._ui_call(lambda p=pid, i=idx: self._append_log(f"[{i}/{len(playlist_ids)}] {p}"))
                try:
                    videos = self.youtube_service.fetch_playlist_items(pid, search_text=api_filter)
                except Exception as exc:
                    self._ui_call(
                        lambda p=pid, e=exc: self._append_log(f"HATA: Playlist okunamadı ({p}): {e}")
                    )
                    continue

                total_raw += len(videos)
                for video in videos:
                    if video.video_id in seen_ids:
                        duplicate_skipped += 1
                        continue
                    seen_ids.add(video.video_id)
                    merged.append(video)

            def finish_fetch() -> None:
                self.all_videos = merged
                self._refresh_visible_videos()
                self._append_log(
                    (
                        f"Listeleme tamamlandı. Ham: {total_raw}, "
                        f"Tekil: {len(merged)}, Duplike atlanan: {duplicate_skipped}"
                    )
                )
                messagebox.showinfo(
                    "Listeleme Tamamlandı",
                    (
                        f"Toplam tekil video: {len(merged)}\n"
                        f"Ham sonuç: {total_raw}\n"
                        f"Duplike atlanan: {duplicate_skipped}"
                    ),
                )

            self._ui_call(finish_fetch)

        self._run_async("Video çekme", worker)

    def _refresh_visible_videos(self) -> None:
        terms = build_search_terms(self.search_entry.get().strip())
        self.visible_videos = [v for v in self.all_videos if title_matches_terms(v.title, terms)]

        self.listbox.delete(0, tk.END)
        for idx, video in enumerate(self.visible_videos, start=1):
            self.listbox.insert(tk.END, f"{idx:4d}. {video.title}")

        self._update_stats()

    def _update_stats(self) -> None:
        selected_count = len(self.listbox.curselection())
        self.stats_var.set(
            f"Toplam: {len(self.all_videos)} | Görünen: {len(self.visible_videos)} | Seçili: {selected_count}"
        )

    def select_all_visible(self) -> None:
        self.listbox.selection_set(0, tk.END)
        self._update_stats()

    def clear_selection(self) -> None:
        self.listbox.selection_clear(0, tk.END)
        self._update_stats()

    def _collect_selected_video_ids(self) -> list[str]:
        selected_indices = list(self.listbox.curselection())
        if selected_indices:
            return [self.visible_videos[i].video_id for i in selected_indices if i < len(self.visible_videos)]

        range_text = self.range_entry.get().strip()
        if not range_text:
            raise ValueError("Seçim veya aralık belirtmelisiniz.")

        indices = parse_range_string(range_text, max_index=len(self.visible_videos))
        return [self.visible_videos[i - 1].video_id for i in indices]

    def transfer_selected(self) -> None:
        if not self.visible_videos:
            messagebox.showerror("Eksik İşlem", "Önce playlist videolarını getiriniz.")
            return

        try:
            video_ids = self._collect_selected_video_ids()
        except ValueError as exc:
            messagebox.showerror("Hatalı Girdi", str(exc))
            return

        if not video_ids:
            messagebox.showerror("Eksik İşlem", "Eklenecek video bulunamadı.")
            return

        target_playlist_id = extract_playlist_id(self.target_entry.get().strip() or "")
        target_playlist_name = self.target_name_entry.get().strip()
        dry_run = self.dry_run_var.get()
        transfer_state_path = get_app_dir() / DEFAULT_TRANSFER_STATE_NAME
        resume_transfer = False
        if not dry_run and transfer_state_path.exists():
            resume_transfer = messagebox.askyesno(
                "Transferi Devam Ettir",
                "Yarım kalan transfer state bulundu. Kaldığı yerden devam edilsin mi?",
            )
            if not resume_transfer:
                transfer_state_path.unlink(missing_ok=True)

        def worker() -> None:
            if not self._ensure_connected():
                return

            mode_text = "dry-run" if dry_run else "gerçek transfer"
            self._ui_call(
                lambda: self._append_log(f"Transfer başlatıldı ({mode_text}). İstek sayısı: {len(video_ids)}")
            )
            try:
                if dry_run:
                    stats = self.youtube_service.preview_add_videos(
                        video_ids=video_ids, target_playlist_id=target_playlist_id
                    )
                else:
                    stats = self.youtube_service.add_videos_to_playlist(
                        video_ids=video_ids,
                        target_playlist_id=target_playlist_id,
                        target_playlist_name=target_playlist_name,
                        cancel_requested=self.cancel_event.is_set,
                        progress_cb=lambda i, total: self._ui_call(lambda: self._set_progress(i, total)),
                        resume_from_state=resume_transfer,
                        state_path=transfer_state_path,
                    )
            except Exception as exc:
                self._ui_call(lambda: messagebox.showerror("Transfer Hatası", str(exc)))
                self._ui_call(lambda: self._append_log(f"HATA: Transfer başarısız: {exc}"))
                return

            self._ui_call(lambda s=stats, d=dry_run: self._finish_transfer(s, d))

        self._run_async("Video transferi", worker)

    def _finish_transfer(self, stats: TransferStats, dry_run: bool) -> None:
        title = "Transfer Önizleme (Dry-run)" if dry_run else "Transfer Tamamlandı"
        action_label = "Eklenecek (tahmini)" if dry_run else "Eklenen"
        messagebox.showinfo(
            title,
            (
                f"Hedef playlist: {stats.target_playlist_id}\n"
                f"Yeni oluşturuldu: {'Evet' if stats.target_created else 'Hayır'}\n"
                f"State'den devam: {'Evet' if stats.resumed_from_state else 'Hayır'}\n"
                f"İptal edildi: {'Evet' if stats.cancelled else 'Hayır'}\n"
                f"İstenen: {stats.requested_count}\n"
                f"{action_label}: {stats.added_count}\n"
                f"Duplike atlanan: {stats.skipped_duplicate_count}\n"
                f"Hata: {stats.failed_count}"
            ),
        )
        if dry_run:
            self._append_log(
                (
                    "Dry-run özeti -> "
                    f"Eklenecek: {stats.added_count}, "
                    f"Duplike: {stats.skipped_duplicate_count}"
                )
            )
            append_history(
                "transfer_dry_run",
                {
                    "target_playlist_id": stats.target_playlist_id,
                    "requested_count": stats.requested_count,
                    "would_add_count": stats.added_count,
                    "skipped_duplicate_count": stats.skipped_duplicate_count,
                    "resumed_from_state": stats.resumed_from_state,
                },
            )
            return
        self._append_log(
            (
                "Transfer özeti -> "
                f"Eklenen: {stats.added_count}, "
                f"Duplike: {stats.skipped_duplicate_count}, "
                f"Hata: {stats.failed_count}"
            )
        )
        append_history(
            "transfer_completed",
            {
                "target_playlist_id": stats.target_playlist_id,
                "requested_count": stats.requested_count,
                "added_count": stats.added_count,
                "skipped_duplicate_count": stats.skipped_duplicate_count,
                "failed_count": stats.failed_count,
                "resumed_from_state": stats.resumed_from_state,
                "cancelled": stats.cancelled,
            },
        )

    def download_pdfs(self) -> None:
        if not self.visible_videos:
            messagebox.showerror("Eksik İşlem", "Önce playlist videolarını getiriniz.")
            return

        initial_dir = self.prefs.last_download_dir or str(Path.cwd())
        selected_dir = filedialog.askdirectory(title="PDF çıktısı klasörü", initialdir=initial_dir)
        if not selected_dir:
            return

        self.prefs.last_download_dir = selected_dir
        output_dir = Path(selected_dir)
        state_path = output_dir / DEFAULT_JOB_STATE_NAME
        resume_from_state = False
        if state_path.exists():
            resume_from_state = messagebox.askyesno(
                "Devam Et",
                "Önceki PDF işi için kayıt bulundu. Kaldığı yerden devam edilsin mi?",
            )

        def worker() -> None:
            if not self._ensure_connected():
                return

            video_ids = [v.video_id for v in self.visible_videos]
            descriptions = self.youtube_service.fetch_video_descriptions(
                video_ids=video_ids,
                cancel_requested=self.cancel_event.is_set,
                progress_cb=lambda i, total: self._ui_call(lambda: self._set_progress(i, total)),
            )

            report = self.pdf_service.process_videos(
                videos=self.visible_videos,
                descriptions=descriptions,
                output_dir=output_dir,
                cancel_requested=self.cancel_event.is_set,
                progress_cb=lambda i, total: self._ui_call(lambda: self._set_progress(i, total)),
                resume_from_state=resume_from_state,
            )
            self._ui_call(lambda r=report: self._finish_pdf(r))

        self._run_async("PDF indirme", worker)

    def _finish_pdf(self, report) -> None:
        messagebox.showinfo(
            "PDF İşlemi Tamamlandı",
            (
                f"Toplam video: {report.total_videos}\n"
                f"Link bulunan video: {report.videos_with_links}\n"
                f"Bulunan link: {report.discovered_links}\n"
                f"İndirilen PDF: {report.downloaded_pdfs}\n"
                f"Başarısız link: {len(report.failed_links)}\n"
                f"ZIP: {report.zip_path or 'Yok'}\n"
                f"Rapor: pdf_report.txt\n"
                f"Başarısız link dosyası: failed_links.txt"
            ),
        )
        self._append_log(
            f"PDF özeti -> İndirilen: {report.downloaded_pdfs}, Başarısız: {len(report.failed_links)}"
        )
        append_history(
            "pdf_job_completed",
            {
                "total_videos": report.total_videos,
                "videos_with_links": report.videos_with_links,
                "downloaded_pdfs": report.downloaded_pdfs,
                "failed_links": len(report.failed_links),
                "zip_path": report.zip_path,
                "resumed_from_state": report.resumed_from_state,
                "cancelled": report.cancelled,
            },
        )

    def export_log(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt")],
            title="Log dışa aktar",
        )
        if not path:
            return
        Path(path).write_text(self.log_text.get("1.0", tk.END), encoding="utf-8")
        messagebox.showinfo("Tamam", f"Log dışa aktarıldı:\n{path}")

    def export_videos_csv(self) -> None:
        if not self.visible_videos:
            messagebox.showwarning("Boş Liste", "Dışa aktarılacak video listesi yok.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            title="Videoları CSV dışa aktar",
        )
        if not path:
            return

        with Path(path).open("w", newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["index", "title", "video_id", "source_playlist_id"])
            for idx, video in enumerate(self.visible_videos, start=1):
                writer.writerow([idx, video.title, video.video_id, video.source_playlist_id])
        messagebox.showinfo("Tamam", f"CSV oluşturuldu:\n{path}")

    def open_settings_dialog(self) -> None:
        """Open settings for timeout/retry/backoff and dry-run defaults."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Ayarlar")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.configure(bg=self.theme.bg)
        dialog.resizable(False, False)

        frame = ttk.Frame(dialog, style="Panel.TFrame", padding=12)
        frame.pack(fill="both", expand=True)

        timeout_var = tk.StringVar(value=str(self.prefs.request_timeout_seconds))
        retry_var = tk.StringVar(value=str(self.prefs.retry_total))
        backoff_var = tk.StringVar(value=str(self.prefs.retry_backoff_factor))
        throttle_var = tk.StringVar(value=str(self.prefs.transfer_throttle_seconds))
        dry_run_default_var = tk.BooleanVar(value=self.dry_run_var.get())
        housekeeping_enabled_var = tk.BooleanVar(value=self.prefs.startup_housekeeping_enabled)
        retention_days_var = tk.StringVar(value=str(self.prefs.history_retention_days))
        weekly_archive_var = tk.BooleanVar(value=self.prefs.weekly_auto_archive_enabled)
        history_max_size_var = tk.StringVar(value=str(self.prefs.history_max_size_mb))
        archive_max_files_var = tk.StringVar(value=str(self.prefs.archive_max_files))

        ttk.Label(frame, text="Request timeout (sn):", style="Panel.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=timeout_var, width=20).grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Retry sayısı:", style="Panel.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=retry_var, width=20).grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Backoff çarpanı:", style="Panel.TLabel").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=backoff_var, width=20).grid(row=2, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Transfer throttle (sn):", style="Panel.TLabel").grid(
            row=3, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=throttle_var, width=20).grid(row=3, column=1, sticky="w", pady=4)

        ttk.Checkbutton(
            frame,
            text="Transfer için dry-run varsayılan açık",
            variable=dry_run_default_var,
            style="Panel.TLabel",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 4))

        ttk.Checkbutton(
            frame,
            text="Açılışta housekeeping (history rotate)",
            variable=housekeeping_enabled_var,
            style="Panel.TLabel",
        ).grid(row=5, column=0, columnspan=2, sticky="w", pady=(4, 4))

        ttk.Label(frame, text="History retention (gün):", style="Panel.TLabel").grid(
            row=6, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=retention_days_var, width=20).grid(
            row=6, column=1, sticky="w", pady=4
        )

        ttk.Checkbutton(
            frame,
            text="Haftalık otomatik history arşivle",
            variable=weekly_archive_var,
            style="Panel.TLabel",
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 4))

        ttk.Label(frame, text="History max size (MB):", style="Panel.TLabel").grid(
            row=8, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=history_max_size_var, width=20).grid(
            row=8, column=1, sticky="w", pady=4
        )

        ttk.Label(frame, text="Max archive file sayısı:", style="Panel.TLabel").grid(
            row=9, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=archive_max_files_var, width=20).grid(
            row=9, column=1, sticky="w", pady=4
        )

        button_row = ttk.Frame(frame, style="Panel.TFrame")
        button_row.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(12, 0))

        def save_settings() -> None:
            try:
                timeout = int(timeout_var.get().strip())
                retries = int(retry_var.get().strip())
                backoff = float(backoff_var.get().strip())
                throttle = float(throttle_var.get().strip())
                retention_days = int(retention_days_var.get().strip())
                history_max_size_mb = float(history_max_size_var.get().strip())
                archive_max_files = int(archive_max_files_var.get().strip())
            except ValueError:
                messagebox.showerror(
                    "Hatalı Ayar",
                    "Timeout/retry/backoff/throttle/retention/max_size/archive_count alanları sayısal olmalı.",
                )
                return

            if (
                timeout < 5
                or retries < 0
                or backoff <= 0
                or throttle < 0
                or retention_days < 1
                or history_max_size_mb <= 0
                or archive_max_files < 1
            ):
                messagebox.showerror(
                    "Hatalı Ayar",
                    "Timeout >= 5, retry >= 0, backoff > 0, throttle >= 0, retention >= 1, max_size > 0, archive_count >= 1 olmalıdır.",
                )
                return

            self.prefs.request_timeout_seconds = timeout
            self.prefs.retry_total = retries
            self.prefs.retry_backoff_factor = backoff
            self.prefs.transfer_throttle_seconds = throttle
            self.dry_run_var.set(dry_run_default_var.get())
            self.prefs.transfer_dry_run = self.dry_run_var.get()
            self.prefs.startup_housekeeping_enabled = housekeeping_enabled_var.get()
            self.prefs.history_retention_days = retention_days
            self.prefs.weekly_auto_archive_enabled = weekly_archive_var.get()
            self.prefs.history_max_size_mb = history_max_size_mb
            self.prefs.archive_max_files = archive_max_files

            self.youtube_service.update_retry_policy(
                timeout_seconds=timeout,
                retry_total=retries,
                retry_backoff_factor=backoff,
                transfer_throttle_seconds=throttle,
            )
            self.pdf_service.update_retry_policy(
                timeout_seconds=timeout,
                retry_total=retries,
                retry_backoff_factor=backoff,
            )
            save_preferences(self._current_preferences())
            self._append_log(
                (
                    "Ayarlar güncellendi: "
                    f"timeout={timeout}s retry={retries} "
                    f"backoff={backoff} throttle={throttle}s "
                    f"startup_housekeeping={self.prefs.startup_housekeeping_enabled} "
                    f"retention_days={retention_days} "
                    f"weekly_archive={self.prefs.weekly_auto_archive_enabled} "
                    f"max_size_mb={history_max_size_mb} "
                    f"archive_max_files={archive_max_files}"
                )
            )
            dialog.destroy()

        ttk.Button(button_row, text="Kaydet", command=save_settings).pack(side="left", padx=(0, 6))
        ttk.Button(button_row, text="Vazgeç", command=dialog.destroy).pack(side="left")

    def show_retry_details_dialog(self) -> None:
        """Show retry/backoff diagnostics for the latest operation."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Retry Detayları")
        dialog.transient(self.root)
        dialog.configure(bg=self.theme.bg)
        dialog.geometry("900x380")

        frame = ttk.Frame(dialog, style="Panel.TFrame", padding=10)
        frame.pack(fill="both", expand=True)
        text = tk.Text(frame, bg=self.theme.entry_bg, fg=self.theme.fg, wrap="word", relief="flat")
        text.pack(fill="both", expand=True)

        if not self.last_retry_events:
            text.insert("1.0", "Son işlem için retry detayı yok.")
        else:
            for evt in self.last_retry_events:
                status = evt.status if evt.status is not None else "-"
                text.insert(
                    tk.END,
                    (
                        f"op={evt.operation} attempt={evt.attempt}/{evt.max_attempts} "
                        f"status={status} delay={evt.delay_seconds:.2f}s\n"
                        f"error={evt.error}\n\n"
                    ),
                )
        text.configure(state="disabled")

    def show_maintenance_dialog(self) -> None:
        """Show startup housekeeping results and quick maintenance actions."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Maintenance")
        dialog.transient(self.root)
        dialog.configure(bg=self.theme.bg)
        dialog.geometry("940x560")

        frame = ttk.Frame(dialog, style="Panel.TFrame", padding=10)
        frame.pack(fill="both", expand=True)

        top = ttk.Frame(frame, style="Panel.TFrame")
        top.pack(fill="both", expand=True)

        text = tk.Text(top, bg=self.theme.entry_bg, fg=self.theme.fg, wrap="word", relief="flat", height=14)
        text.pack(fill="both", expand=True, pady=(0, 8))

        archive_panel = ttk.Frame(top, style="Panel.TFrame")
        archive_panel.pack(fill="both", expand=True)
        ttk.Label(archive_panel, text="Arşiv Dosyaları:", style="Panel.TLabel").pack(anchor="w")
        self.archive_listbox = tk.Listbox(
            archive_panel,
            selectmode=tk.EXTENDED,
            bg=self.theme.entry_bg,
            fg=self.theme.fg,
            selectbackground=self.theme.accent,
            selectforeground=self.theme.fg,
            relief="flat",
            height=8,
        )
        self.archive_listbox.pack(fill="both", expand=True)

        def collect_health_report() -> dict[str, object]:
            trend_entries = [
                item for item in load_history(limit=200) if item.get("event") == "startup_housekeeping"
            ][:5]
            trend: list[dict[str, object]] = []
            for item in trend_entries:
                payload = item.get("payload", {})
                trend.append(
                    {
                        "timestamp": item.get("timestamp", "-"),
                        "removed": payload.get("removed", "0"),
                        "archive_created": payload.get("archive_created", "Hayır"),
                        "archives_removed": payload.get("archives_removed", "0"),
                    }
                )
            report: dict[str, object] = {
                "housekeeping": dict(self.last_housekeeping_report),
                "history_file_size_mb": round(get_history_file_size_mb(), 4),
                "archive_file_count": len(list_archive_files()),
                "housekeeping_trend_last_5": trend,
                "preferences": {
                    "startup_housekeeping_enabled": self.prefs.startup_housekeeping_enabled,
                    "history_retention_days": self.prefs.history_retention_days,
                    "weekly_auto_archive_enabled": self.prefs.weekly_auto_archive_enabled,
                    "history_max_size_mb": self.prefs.history_max_size_mb,
                    "archive_max_files": self.prefs.archive_max_files,
                    "last_history_archive_at": self.prefs.last_history_archive_at,
                },
            }
            return report

        archive_file_paths: list[Path] = []

        def refresh_archive_list() -> None:
            archive_file_paths.clear()
            archive_file_paths.extend(list_archive_files())
            self.archive_listbox.delete(0, tk.END)
            for path in archive_file_paths:
                size_kb = path.stat().st_size / 1024
                self.archive_listbox.insert(tk.END, f"{path.name} ({size_kb:.1f} KB)")

        def render_report() -> None:
            report = collect_health_report()
            text.configure(state="normal")
            text.delete("1.0", tk.END)
            text.insert(tk.END, "Son Startup Housekeeping Raporu\n")
            text.insert(tk.END, "-" * 38 + "\n")
            for key, value in report.get("housekeeping", {}).items():
                text.insert(tk.END, f"{key}: {value}\n")
            text.insert(tk.END, f"\nAktif history boyutu (MB): {report['history_file_size_mb']}\n")
            text.insert(tk.END, f"Arşiv dosya sayısı: {report['archive_file_count']}\n")
            text.insert(tk.END, "\nSon 5 Housekeeping Trendi:\n")
            for item in report.get("housekeeping_trend_last_5", []):
                text.insert(
                    tk.END,
                    (
                        f"- {item['timestamp']} | removed={item['removed']} "
                        f"| archive_created={item['archive_created']} "
                        f"| archives_removed={item['archives_removed']}\n"
                    ),
                )
            text.configure(state="disabled")
            refresh_archive_list()

        def run_housekeeping_now() -> None:
            self._run_startup_housekeeping()
            self.root.after(700, render_report)
            messagebox.showinfo("Tamam", "Maintenance housekeeping arka planda başlatıldı.")

        def export_health_report(as_json: bool) -> None:
            report = collect_health_report()
            if as_json:
                path = filedialog.asksaveasfilename(
                    defaultextension=".json",
                    filetypes=[("JSON", "*.json")],
                    title="Health report JSON dışa aktar",
                )
                if not path:
                    return
                Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            else:
                path = filedialog.asksaveasfilename(
                    defaultextension=".txt",
                    filetypes=[("Text", "*.txt")],
                    title="Health report TXT dışa aktar",
                )
                if not path:
                    return
                lines = ["Maintenance Health Report", "-" * 28]
                for key, value in report.get("housekeeping", {}).items():
                    lines.append(f"{key}: {value}")
                lines.append(f"history_file_size_mb: {report['history_file_size_mb']}")
                lines.append(f"archive_file_count: {report['archive_file_count']}")
                lines.append(f"preferences: {report['preferences']}")
                Path(path).write_text("\n".join(lines), encoding="utf-8")
            messagebox.showinfo("Tamam", f"Health report dışa aktarıldı:\n{path}")

        def open_archive_folder() -> None:
            archive_dir = get_archive_dir()
            try:
                os.startfile(str(archive_dir))  # type: ignore[attr-defined]
            except Exception as exc:
                messagebox.showerror("Hata", f"Arşiv klasörü açılamadı: {exc}")

        def delete_selected_archives() -> None:
            selected = list(self.archive_listbox.curselection())
            if not selected:
                messagebox.showwarning("Seçim Yok", "Silmek için en az bir arşiv dosyası seçin.")
                return
            if not messagebox.askyesno("Onay", "Seçili arşiv dosyaları silinsin mi?"):
                return
            removed = 0
            for index in selected:
                if 0 <= index < len(archive_file_paths):
                    archive_file_paths[index].unlink(missing_ok=True)
                    removed += 1
            refresh_archive_list()
            render_report()
            messagebox.showinfo("Tamam", f"{removed} arşiv dosyası silindi.")

        def preview_selected_archive() -> None:
            selected = list(self.archive_listbox.curselection())
            if len(selected) != 1:
                messagebox.showwarning("Seçim", "Önizleme için tek bir arşiv dosyası seçin.")
                return
            index = selected[0]
            if index < 0 or index >= len(archive_file_paths):
                messagebox.showerror("Hata", "Seçilen arşiv dosyası bulunamadı.")
                return
            target = archive_file_paths[index]
            try:
                lines = target.read_text(encoding="utf-8").splitlines()
            except OSError as exc:
                messagebox.showerror("Hata", f"Arşiv okunamadı: {exc}")
                return

            preview = tk.Toplevel(dialog)
            preview.title(f"Arşiv Önizleme - {target.name}")
            preview.transient(dialog)
            preview.geometry("940x560")
            preview.configure(bg=self.theme.bg)

            controls = ttk.Frame(preview, style="Panel.TFrame", padding=(10, 8))
            controls.pack(fill="x")
            ttk.Label(controls, text="Ara:", style="Panel.TLabel").pack(side="left")
            search_var = tk.StringVar()
            search_entry = ttk.Entry(controls, textvariable=search_var, width=40)
            search_entry.pack(side="left", padx=(6, 10))
            pretty_var = tk.BooleanVar(value=False)
            line_numbers_var = tk.BooleanVar(value=True)
            view_mode_var = tk.StringVar(value="raw")
            ttk.Checkbutton(
                controls,
                text="JSONL pretty göster",
                variable=pretty_var,
                style="Panel.TLabel",
            ).pack(side="left")
            ttk.Checkbutton(
                controls,
                text="Satır numarası",
                variable=line_numbers_var,
                style="Panel.TLabel",
            ).pack(side="left", padx=(8, 0))
            ttk.Label(controls, text="Görünüm:", style="Panel.TLabel").pack(side="left", padx=(8, 4))
            view_mode = ttk.Combobox(
                controls,
                textvariable=view_mode_var,
                values=["raw", "pretty", "compare"],
                width=10,
                state="readonly",
            )
            view_mode.pack(side="left")

            preview_text = tk.Text(
                preview,
                bg=self.theme.entry_bg,
                fg=self.theme.fg,
                wrap="word",
                relief="flat",
            )
            preview_text.pack(fill="both", expand=True, padx=10, pady=10)

            max_lines = 120

            def to_pretty(line: str) -> str:
                try:
                    return json.dumps(json.loads(line), ensure_ascii=False, indent=2)
                except json.JSONDecodeError:
                    return line

            def format_with_line_no(idx: int, content: str) -> str:
                if line_numbers_var.get():
                    return f"{idx:04d}: {content}"
                return content

            def copy_visible_text() -> None:
                current = preview_text.get("1.0", tk.END).rstrip("\n")
                if not current.strip():
                    messagebox.showwarning("Boş", "Kopyalanacak içerik yok.")
                    return
                preview.clipboard_clear()
                preview.clipboard_append(current)
                messagebox.showinfo("Tamam", "Görünen içerik panoya kopyalandı.")

            ttk.Button(controls, text="Görüneni Kopyala", command=copy_visible_text).pack(
                side="right", padx=(6, 0)
            )

            def render_preview() -> None:
                preview_text.configure(state="normal")
                preview_text.delete("1.0", tk.END)
                if not lines:
                    preview_text.insert("1.0", "Arşiv dosyası boş.")
                    preview_text.configure(state="disabled")
                    return

                content_lines = lines[:max_lines]
                mode = view_mode_var.get().strip()
                rendered_lines: list[str] = []
                for idx, line in enumerate(content_lines, start=1):
                    raw = format_with_line_no(idx, line)
                    pretty = format_with_line_no(idx, to_pretty(line))
                    if mode == "pretty" or (pretty_var.get() and mode == "raw"):
                        rendered_lines.append(pretty)
                    elif mode == "compare":
                        rendered_lines.append(f"[RAW ] {raw}")
                        rendered_lines.append(f"[PRETTY] {pretty}")
                        rendered_lines.append("-" * 60)
                    else:
                        rendered_lines.append(raw)

                preview_text.insert("1.0", "\n".join(rendered_lines))

                if len(lines) > max_lines:
                    preview_text.insert(
                        tk.END,
                        f"\n\n... (toplam {len(lines)} satır, sadece ilk {max_lines} gösterildi)",
                    )
                preview_text.configure(state="disabled")
                highlight_search_matches()

            def highlight_search_matches() -> None:
                needle = search_var.get().strip()
                preview_text.tag_remove("match", "1.0", tk.END)
                if not needle:
                    return
                preview_text.tag_configure("match", background="#5A4B00", foreground="#FFFFFF")
                start = "1.0"
                while True:
                    idx = preview_text.search(needle, start, stopindex=tk.END, nocase=True)
                    if not idx:
                        break
                    end = f"{idx}+{len(needle)}c"
                    preview_text.tag_add("match", idx, end)
                    start = end

            search_entry.bind("<KeyRelease>", lambda _: highlight_search_matches())
            pretty_var.trace_add("write", lambda *_: render_preview())
            line_numbers_var.trace_add("write", lambda *_: render_preview())
            view_mode.bind("<<ComboboxSelected>>", lambda _: render_preview())
            render_preview()

        def export_selected_archive() -> None:
            selected = list(self.archive_listbox.curselection())
            if len(selected) != 1:
                messagebox.showwarning("Seçim", "Dışa aktarma için tek bir arşiv dosyası seçin.")
                return
            index = selected[0]
            if index < 0 or index >= len(archive_file_paths):
                messagebox.showerror("Hata", "Seçilen arşiv dosyası bulunamadı.")
                return
            source = archive_file_paths[index]
            path = filedialog.asksaveasfilename(
                defaultextension=".jsonl",
                filetypes=[("JSONL", "*.jsonl"), ("All Files", "*.*")],
                initialfile=source.name,
                title="Seçili arşivi dışa aktar",
            )
            if not path:
                return
            try:
                Path(path).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
            except OSError as exc:
                messagebox.showerror("Hata", f"Arşiv dışa aktarılamadı: {exc}")
                return
            messagebox.showinfo("Tamam", f"Arşiv dışa aktarıldı:\n{path}")

        buttons = ttk.Frame(frame, style="Panel.TFrame")
        buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(buttons, text="Housekeeping Şimdi Çalıştır", command=run_housekeeping_now).pack(
            side="left"
        )
        ttk.Button(buttons, text="Raporu TXT Aktar", command=lambda: export_health_report(False)).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Raporu JSON Aktar", command=lambda: export_health_report(True)).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Arşiv Klasörünü Aç", command=open_archive_folder).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Seçili Arşivi Dışa Aktar", command=export_selected_archive).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Seçili Arşivi Önizle", command=preview_selected_archive).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Seçili Arşivleri Sil", command=delete_selected_archives).pack(
            side="left", padx=(6, 0)
        )

        render_report()

    def show_history_dialog(self) -> None:
        """Show filterable operation history and optional CSV export."""
        entries = load_history(limit=500)
        dialog = tk.Toplevel(self.root)
        dialog.title("İşlem Geçmişi")
        dialog.transient(self.root)
        dialog.configure(bg=self.theme.bg)
        dialog.geometry("940x520")

        root_frame = ttk.Frame(dialog, style="Panel.TFrame", padding=10)
        root_frame.pack(fill="both", expand=True)

        controls = ttk.Frame(root_frame, style="Panel.TFrame")
        controls.pack(fill="x", pady=(0, 8))

        event_values = ["all"] + sorted({item.get("event", "") for item in entries if item.get("event")})
        event_var = tk.StringVar(value="all")
        search_var = tk.StringVar()

        ttk.Label(controls, text="Event:", style="Panel.TLabel").pack(side="left")
        event_combo = ttk.Combobox(
            controls, textvariable=event_var, values=event_values, width=30, state="readonly"
        )
        event_combo.pack(side="left", padx=(6, 10))

        ttk.Label(controls, text="Ara:", style="Panel.TLabel").pack(side="left")
        search_entry = ttk.Entry(controls, textvariable=search_var, width=40)
        search_entry.pack(side="left", padx=(6, 10))

        text = tk.Text(root_frame, bg=self.theme.entry_bg, fg=self.theme.fg, wrap="word", relief="flat")
        text.pack(fill="both", expand=True)

        filtered_entries: list[dict] = []

        def reload_entries() -> None:
            entries.clear()
            entries.extend(load_history(limit=500))

        def apply_filter() -> None:
            text.configure(state="normal")
            text.delete("1.0", tk.END)
            selected_event = event_var.get().strip()
            needle = search_var.get().strip().lower()

            filtered_entries.clear()
            for item in entries:
                event = str(item.get("event", ""))
                payload = item.get("payload", {})
                payload_str = str(payload)
                if selected_event != "all" and event != selected_event:
                    continue
                if needle and needle not in event.lower() and needle not in payload_str.lower():
                    continue
                filtered_entries.append(item)

            if not filtered_entries:
                text.insert("1.0", "Filtre sonucu kayıt bulunamadı.")
            else:
                for item in filtered_entries:
                    ts = item.get("timestamp", "-")
                    event = item.get("event", "-")
                    payload = item.get("payload", {})
                    text.insert(tk.END, f"[{ts}] {event}\n{payload}\n\n")
            text.configure(state="disabled")

        def export_filtered_csv() -> None:
            if not filtered_entries:
                messagebox.showwarning("Boş Sonuç", "Dışa aktarılacak geçmiş kaydı yok.")
                return
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                title="Geçmişi CSV dışa aktar",
            )
            if not path:
                return
            with Path(path).open("w", newline="", encoding="utf-8-sig") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["timestamp", "event", "payload"])
                for item in filtered_entries:
                    writer.writerow([item.get("timestamp", ""), item.get("event", ""), item.get("payload", {})])
            messagebox.showinfo("Tamam", f"Geçmiş CSV dışa aktarıldı:\n{path}")

        def archive_current_history() -> None:
            archive_path = archive_history()
            if archive_path is None:
                messagebox.showinfo("Bilgi", "Arşivlenecek geçmiş kaydı bulunamadı.")
                return
            reload_entries()
            apply_filter()
            messagebox.showinfo("Tamam", f"Geçmiş arşivlendi:\n{archive_path}")

        def rotate_old_history() -> None:
            kept, removed = rotate_history(days=self.prefs.history_retention_days)
            reload_entries()
            apply_filter()
            messagebox.showinfo(
                "Rotate Tamamlandı",
                (
                    f"{self.prefs.history_retention_days} günden eski kayıtlar temizlendi.\n"
                    f"Kalan: {kept}\nSilinen: {removed}"
                ),
            )

        def clear_state_files_action() -> None:
            extra_state = []
            if self.prefs.last_download_dir:
                extra_state.append(Path(self.prefs.last_download_dir) / DEFAULT_JOB_STATE_NAME)
            removed = clear_runtime_state_files(extra_paths=extra_state)
            if removed:
                removed_lines = "\n".join(str(item) for item in removed)
                messagebox.showinfo("Tamam", f"State dosyaları silindi:\n{removed_lines}")
            else:
                messagebox.showinfo("Bilgi", "Silinecek state dosyası bulunamadı.")

        buttons = ttk.Frame(root_frame, style="Panel.TFrame")
        buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(buttons, text="Filtreyi Uygula", command=apply_filter).pack(side="left")
        ttk.Button(buttons, text="Filtreyi CSV Aktar", command=export_filtered_csv).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Geçmişi Arşivle", command=archive_current_history).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="30+ Gün Rotate", command=rotate_old_history).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="State Dosyalarını Temizle", command=clear_state_files_action).pack(
            side="left", padx=(6, 0)
        )

        event_combo.bind("<<ComboboxSelected>>", lambda _: apply_filter())
        search_entry.bind("<KeyRelease>", lambda _: apply_filter())
        apply_filter()
