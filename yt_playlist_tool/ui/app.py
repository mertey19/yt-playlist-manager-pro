"""Main desktop interface built with ttk."""

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
    clear_state_files,
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
    """Coordinates UI and workflow control for the playlist tool."""

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
        self.last_housekeeping_report: dict[str, str] = {"status": "Not run yet."}

        self.all_videos: list[VideoItem] = []
        self.visible_videos: list[VideoItem] = []

        self.status_var = tk.StringVar(value="Ready")
        self.stats_var = tk.StringVar(value="Total: 0 | Visible: 0 | Selected: 0")
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
            text="Source Playlist ID/URL (line-by-line or comma-separated):",
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

        ttk.Label(top, text="Target Playlist ID (leave empty to create new):", style="Panel.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.target_entry = ttk.Entry(top, width=60)
        self.target_entry.grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(top, text="New Playlist Name:", style="Panel.TLabel").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.target_name_entry = ttk.Entry(top, width=60)
        self.target_name_entry.grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(top, text="Title filter:", style="Panel.TLabel").grid(
            row=3, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.search_entry = ttk.Entry(top, width=60)
        self.search_entry.grid(row=3, column=1, sticky="ew", pady=4)
        self.search_entry.bind("<KeyRelease>", lambda _: self._refresh_visible_videos())

        ttk.Label(top, text="Range (e.g. 1-5, 10, 15-20):", style="Panel.TLabel").grid(
            row=4, column=0, sticky="w", padx=(0, 8), pady=4
        )
        self.range_entry = ttk.Entry(top, width=60)
        self.range_entry.grid(row=4, column=1, sticky="ew", pady=4)

        top.columnconfigure(1, weight=1)

        buttons = ttk.Frame(main, style="Panel.TFrame", padding=8)
        buttons.pack(fill="x", pady=(0, 8))

        self.fetch_button = ttk.Button(
            buttons, text="Fetch Playlist Videos", command=self.fetch_videos
        )
        self.fetch_button.pack(side="left", padx=(0, 6))
        self.transfer_button = ttk.Button(
            buttons, text="Add Selected/Range Videos", command=self.transfer_selected
        )
        self.transfer_button.pack(side="left", padx=6)
        self.pdf_button = ttk.Button(
            buttons, text="Download PDFs + Create ZIP", command=self.download_pdfs
        )
        self.pdf_button.pack(side="left", padx=6)
        ttk.Checkbutton(
            buttons,
            text="Dry-run transfer",
            variable=self.dry_run_var,
            style="Panel.TLabel",
        ).pack(side="left", padx=(10, 0))
        self.cancel_button = ttk.Button(
            buttons, text="Cancel", command=self.cancel_current_task, style="Danger.TButton"
        )
        self.cancel_button.pack(side="left", padx=6)
        self.cancel_button.configure(state="disabled")
        ttk.Button(buttons, text="Settings", command=self.open_settings_dialog).pack(
            side="left", padx=6
        )

        self.select_all_button = ttk.Button(buttons, text="Select All", command=self.select_all_visible)
        self.select_all_button.pack(side="right", padx=6)
        self.clear_selection_button = ttk.Button(
            buttons, text="Clear Selection", command=self.clear_selection
        )
        self.clear_selection_button.pack(side="right", padx=6)

        list_panel = ttk.Frame(main, style="Panel.TFrame", padding=8)
        list_panel.pack(fill="both", expand=True, pady=(0, 8))
        ttk.Label(list_panel, text="Videos:", style="Panel.TLabel").pack(anchor="w")

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
        self.menu.add_command(label="Select All", command=self.select_all_visible)
        self.menu.add_command(label="Clear Selection", command=self.clear_selection)
        self.listbox.bind("<Button-3>", self._show_context_menu)

        bottom = ttk.Frame(main, style="Panel.TFrame", padding=8)
        bottom.pack(fill="x", pady=(0, 8))

        self.progress = ttk.Progressbar(bottom, mode="determinate")
        self.progress.pack(fill="x", pady=(0, 6))
        ttk.Label(bottom, textvariable=self.stats_var, style="Panel.TLabel").pack(anchor="w")

        action_bar = ttk.Frame(bottom, style="Panel.TFrame")
        action_bar.pack(fill="x", pady=(6, 0))
        ttk.Button(action_bar, text="Export Log as TXT", command=self.export_log).pack(
            side="left", padx=(0, 6)
        )
        ttk.Button(action_bar, text="Export List to CSV", command=self.export_videos_csv).pack(
            side="left"
        )
        ttk.Button(action_bar, text="Operation History", command=self.show_history_dialog).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(action_bar, text="Retry Details", command=self.show_retry_details_dialog).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(action_bar, text="Maintenance", command=self.show_maintenance_dialog).pack(
            side="left", padx=(6, 0)
        )

        log_panel = ttk.Frame(main, style="Panel.TFrame", padding=8)
        log_panel.pack(fill="both", expand=False)
        ttk.Label(log_panel, text="Logs:", style="Panel.TLabel").pack(anchor="w")
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
        """Run startup maintenance tasks in the background."""
        if not self.prefs.startup_housekeeping_enabled:
            self.last_housekeeping_report = {"status": "Startup housekeeping disabled."}
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
                "archive_created": "Yes" if archive_path else "No",
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
                            "Startup housekeeping completed: "
                            f"removed={removed}, archive={'yes' if archive_path else 'no'}, "
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
            if not messagebox.askyesno(
                "Exit", "An operation is still running. Cancel it before exiting?"
            ):
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
                self._ui_call(lambda: messagebox.showerror("YouTube Connection Error", str(exc)))
                self._ui_call(lambda: self._append_log(f"ERROR: Could not connect: {exc}"))
                return False
            self.youtube_connected = True
            self._ui_call(lambda: self._append_log("Connected to YouTube service."))
            return True

    def _run_async(self, task_name: str, worker: Callable[[], None]) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Busy", "Finish or cancel the current operation first.")
            return

        self.cancel_event.clear()
        self._set_busy(True)
        self._set_status(f"{task_name} is running...")

        def run_task_wrapper() -> None:
            try:
                worker()
            finally:
                retry_events = self.youtube_service.consume_retry_events()
                self.last_retry_events = retry_events
                if retry_events:
                    self._ui_call(
                        lambda n=len(retry_events): self._append_log(
                            f"Retry events captured: {n} (see Retry Details)."
                        )
                    )
                self._ui_call(lambda: self._set_busy(False))
                self._ui_call(lambda: self._set_status("Ready"))
                self._ui_call(lambda: self._set_progress(0, 0))

        self.worker_thread = threading.Thread(target=run_task_wrapper, daemon=True)
        self.worker_thread.start()

    def cancel_current_task(self) -> None:
        self.cancel_event.set()
        self._set_status("Cancel request sent...")
        self._append_log("Cancel request received. Operation will stop at a safe point.")

    def fetch_videos(self) -> None:
        playlist_ids = parse_playlist_id_list(self.src_text.get("1.0", tk.END))
        if not playlist_ids:
            messagebox.showerror("Invalid Input", "Please provide at least one valid source playlist.")
            return

        api_filter = self.search_entry.get().strip()

        def worker() -> None:
            if not self._ensure_connected():
                return

            seen_ids: set[str] = set()
            merged: list[VideoItem] = []
            duplicate_skipped = 0
            total_raw = 0

            for playlist_position, playlist_id in enumerate(playlist_ids, start=1):
                if self.cancel_event.is_set():
                    self._ui_call(lambda: self._append_log("Video fetch operation was cancelled."))
                    return
                self._ui_call(lambda p=playlist_position: self._set_progress(p, len(playlist_ids)))
                self._ui_call(
                    lambda p_id=playlist_id, p=playlist_position: self._append_log(
                        f"[{p}/{len(playlist_ids)}] {p_id}"
                    )
                )
                try:
                    videos = self.youtube_service.fetch_playlist_items(
                        playlist_id,
                        search_text=api_filter,
                    )
                except Exception as exc:
                    self._ui_call(
                        lambda p_id=playlist_id, err=exc: self._append_log(
                            f"ERROR: Could not read playlist ({p_id}): {err}"
                        )
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
                        f"Listing completed. Raw: {total_raw}, "
                        f"Unique: {len(merged)}, Duplicates skipped: {duplicate_skipped}"
                    )
                )
                messagebox.showinfo(
                    "Listing Completed",
                    (
                        f"Total unique videos: {len(merged)}\n"
                        f"Raw results: {total_raw}\n"
                        f"Duplicates skipped: {duplicate_skipped}"
                    ),
                )

            self._ui_call(finish_fetch)

        self._run_async("Video fetch", worker)

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
            f"Total: {len(self.all_videos)} | Visible: {len(self.visible_videos)} | Selected: {selected_count}"
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
            raise ValueError("You must provide either a selection or a range.")

        indices = parse_range_string(range_text, max_index=len(self.visible_videos))
        return [self.visible_videos[i - 1].video_id for i in indices]

    def transfer_selected(self) -> None:
        if not self.visible_videos:
            messagebox.showerror("Missing Step", "Fetch playlist videos first.")
            return

        try:
            video_ids = self._collect_selected_video_ids()
        except ValueError as exc:
            messagebox.showerror("Invalid Input", str(exc))
            return

        if not video_ids:
            messagebox.showerror("Missing Step", "No videos found to add.")
            return

        target_playlist_id = extract_playlist_id(self.target_entry.get().strip() or "")
        target_playlist_name = self.target_name_entry.get().strip()
        dry_run = self.dry_run_var.get()
        transfer_state_path = get_app_dir() / DEFAULT_TRANSFER_STATE_NAME
        resume_transfer = False
        if not dry_run and transfer_state_path.exists():
            resume_transfer = messagebox.askyesno(
                "Resume Transfer",
                "A partial transfer state was found. Continue from the saved point?",
            )
            if not resume_transfer:
                transfer_state_path.unlink(missing_ok=True)

        def worker() -> None:
            if not self._ensure_connected():
                return

            mode_text = "dry-run" if dry_run else "live transfer"
            self._ui_call(
                lambda: self._append_log(f"Transfer started ({mode_text}). Request count: {len(video_ids)}")
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
                self._ui_call(lambda: messagebox.showerror("Transfer Error", str(exc)))
                self._ui_call(lambda: self._append_log(f"ERROR: Transfer failed: {exc}"))
                return

            self._ui_call(lambda s=stats, d=dry_run: self._finish_transfer(s, d))

        self._run_async("Video transfer", worker)

    def _finish_transfer(self, stats: TransferStats, dry_run: bool) -> None:
        title = "Transfer Preview (Dry-run)" if dry_run else "Transfer Completed"
        action_label = "Will be added (estimated)" if dry_run else "Added"
        messagebox.showinfo(
            title,
            (
                f"Target playlist: {stats.target_playlist_id}\n"
                f"Created new: {'Yes' if stats.target_created else 'No'}\n"
                f"Resumed from state: {'Yes' if stats.resumed_from_state else 'No'}\n"
                f"Cancelled: {'Yes' if stats.cancelled else 'No'}\n"
                f"Requested: {stats.requested_count}\n"
                f"{action_label}: {stats.added_count}\n"
                f"Duplicates skipped: {stats.skipped_duplicate_count}\n"
                f"Failures: {stats.failed_count}"
            ),
        )
        if dry_run:
            self._append_log(
                (
                    "Dry-run summary -> "
                    f"Will add: {stats.added_count}, "
                    f"Duplicates: {stats.skipped_duplicate_count}"
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
                "Transfer summary -> "
                f"Added: {stats.added_count}, "
                f"Duplicates: {stats.skipped_duplicate_count}, "
                f"Failures: {stats.failed_count}"
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
            messagebox.showerror("Missing Step", "Fetch playlist videos first.")
            return

        initial_dir = self.prefs.last_download_dir or str(Path.cwd())
        selected_dir = filedialog.askdirectory(title="PDF output folder", initialdir=initial_dir)
        if not selected_dir:
            return

        self.prefs.last_download_dir = selected_dir
        output_dir = Path(selected_dir)
        state_path = output_dir / DEFAULT_JOB_STATE_NAME
        resume_from_state = False
        if state_path.exists():
            resume_from_state = messagebox.askyesno(
                "Resume",
                "A saved state was found for a previous PDF job. Continue from where it stopped?",
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

        self._run_async("PDF download", worker)

    def _finish_pdf(self, report) -> None:
        messagebox.showinfo(
            "PDF Job Completed",
            (
                f"Total videos: {report.total_videos}\n"
                f"Videos with links: {report.videos_with_links}\n"
                f"Discovered links: {report.discovered_links}\n"
                f"Downloaded PDFs: {report.downloaded_pdfs}\n"
                f"Failed links: {len(report.failed_links)}\n"
                f"ZIP: {report.zip_path or 'None'}\n"
                f"Report: pdf_report.txt\n"
                f"Failed links file: failed_links.txt"
            ),
        )
        self._append_log(
            f"PDF summary -> Downloaded: {report.downloaded_pdfs}, Failed: {len(report.failed_links)}"
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
            title="Export log",
        )
        if not path:
            return
        Path(path).write_text(self.log_text.get("1.0", tk.END), encoding="utf-8")
        messagebox.showinfo("Done", f"Log exported:\n{path}")

    def export_videos_csv(self) -> None:
        if not self.visible_videos:
            messagebox.showwarning("Empty List", "There is no video list to export.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            title="Export videos to CSV",
        )
        if not path:
            return

        with Path(path).open("w", newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["index", "title", "video_id", "source_playlist_id"])
            for idx, video in enumerate(self.visible_videos, start=1):
                writer.writerow([idx, video.title, video.video_id, video.source_playlist_id])
        messagebox.showinfo("Done", f"CSV created:\n{path}")

    def open_settings_dialog(self) -> None:
        """Open the dialog used to edit network and maintenance settings."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Settings")
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

        ttk.Label(frame, text="Request timeout (sec):", style="Panel.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=timeout_var, width=20).grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Retry count:", style="Panel.TLabel").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=retry_var, width=20).grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Backoff factor:", style="Panel.TLabel").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=backoff_var, width=20).grid(row=2, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Transfer throttle (sn):", style="Panel.TLabel").grid(
            row=3, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=throttle_var, width=20).grid(row=3, column=1, sticky="w", pady=4)

        ttk.Checkbutton(
            frame,
            text="Enable dry-run by default for transfers",
            variable=dry_run_default_var,
            style="Panel.TLabel",
        ).grid(row=4, column=0, columnspan=2, sticky="w", pady=(8, 4))

        ttk.Checkbutton(
            frame,
            text="Run housekeeping at startup (history rotate)",
            variable=housekeeping_enabled_var,
            style="Panel.TLabel",
        ).grid(row=5, column=0, columnspan=2, sticky="w", pady=(4, 4))

        ttk.Label(frame, text="History retention (days):", style="Panel.TLabel").grid(
            row=6, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=retention_days_var, width=20).grid(
            row=6, column=1, sticky="w", pady=4
        )

        ttk.Checkbutton(
            frame,
            text="Enable weekly automatic history archiving",
            variable=weekly_archive_var,
            style="Panel.TLabel",
        ).grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 4))

        ttk.Label(frame, text="History max size (MB):", style="Panel.TLabel").grid(
            row=8, column=0, sticky="w", padx=(0, 8), pady=4
        )
        ttk.Entry(frame, textvariable=history_max_size_var, width=20).grid(
            row=8, column=1, sticky="w", pady=4
        )

        ttk.Label(frame, text="Max archive file count:", style="Panel.TLabel").grid(
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
                    "Invalid Settings",
                    (
                        "Timeout/retry/backoff/throttle/retention/max_size/archive_count "
                        "fields must be numeric."
                    ),
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
                    "Invalid Settings",
                    (
                        "Timeout >= 5, retry >= 0, backoff > 0, throttle >= 0, "
                        "retention >= 1, max_size > 0, archive_count >= 1."
                    ),
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
                    "Settings updated: "
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

        ttk.Button(button_row, text="Save", command=save_settings).pack(side="left", padx=(0, 6))
        ttk.Button(button_row, text="Cancel", command=dialog.destroy).pack(side="left")

    def show_retry_details_dialog(self) -> None:
        """Show retry/backoff details collected from the last operation."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Retry Details")
        dialog.transient(self.root)
        dialog.configure(bg=self.theme.bg)
        dialog.geometry("900x380")

        frame = ttk.Frame(dialog, style="Panel.TFrame", padding=10)
        frame.pack(fill="both", expand=True)
        text = tk.Text(frame, bg=self.theme.entry_bg, fg=self.theme.fg, wrap="word", relief="flat")
        text.pack(fill="both", expand=True)

        if not self.last_retry_events:
            text.insert("1.0", "No retry details were recorded for the last operation.")
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
        """Display maintenance summary, archive tools, and health reporting actions."""
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
        ttk.Label(archive_panel, text="Archive Files:", style="Panel.TLabel").pack(anchor="w")
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
                        "archive_created": payload.get("archive_created", "No"),
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
            text.insert(tk.END, "Latest Startup Housekeeping Report\n")
            text.insert(tk.END, "-" * 38 + "\n")
            for key, value in report.get("housekeeping", {}).items():
                text.insert(tk.END, f"{key}: {value}\n")
            text.insert(tk.END, f"\nActive history size (MB): {report['history_file_size_mb']}\n")
            text.insert(tk.END, f"Archive file count: {report['archive_file_count']}\n")
            text.insert(tk.END, "\nLast 5 housekeeping trend entries:\n")
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
            messagebox.showinfo("Done", "Maintenance housekeeping started in background.")

        def export_health_report(as_json: bool) -> None:
            report = collect_health_report()
            if as_json:
                path = filedialog.asksaveasfilename(
                    defaultextension=".json",
                    filetypes=[("JSON", "*.json")],
                    title="Export health report as JSON",
                )
                if not path:
                    return
                Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            else:
                path = filedialog.asksaveasfilename(
                    defaultextension=".txt",
                    filetypes=[("Text", "*.txt")],
                    title="Export health report as TXT",
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
            messagebox.showinfo("Done", f"Health report exported:\n{path}")

        def open_archive_folder() -> None:
            archive_dir = get_archive_dir()
            try:
                os.startfile(str(archive_dir))  # type: ignore[attr-defined]
            except Exception as exc:
                messagebox.showerror("Error", f"Could not open archive folder: {exc}")

        def delete_selected_archives() -> None:
            selected = list(self.archive_listbox.curselection())
            if not selected:
                messagebox.showwarning("No Selection", "Select at least one archive file to delete.")
                return
            if not messagebox.askyesno("Confirm", "Delete the selected archive files?"):
                return
            removed = 0
            for index in selected:
                if 0 <= index < len(archive_file_paths):
                    archive_file_paths[index].unlink(missing_ok=True)
                    removed += 1
            refresh_archive_list()
            render_report()
            messagebox.showinfo("Done", f"Deleted {removed} archive file(s).")

        def preview_selected_archive() -> None:
            selected = list(self.archive_listbox.curselection())
            if len(selected) != 1:
                messagebox.showwarning("Selection", "Select exactly one archive file for preview.")
                return
            index = selected[0]
            if index < 0 or index >= len(archive_file_paths):
                messagebox.showerror("Error", "Selected archive file was not found.")
                return
            target = archive_file_paths[index]
            try:
                lines = target.read_text(encoding="utf-8").splitlines()
            except OSError as exc:
                messagebox.showerror("Error", f"Could not read archive: {exc}")
                return

            preview = tk.Toplevel(dialog)
            preview.title(f"Archive Preview - {target.name}")
            preview.transient(dialog)
            preview.geometry("940x560")
            preview.configure(bg=self.theme.bg)

            controls = ttk.Frame(preview, style="Panel.TFrame", padding=(10, 8))
            controls.pack(fill="x")
            ttk.Label(controls, text="Search:", style="Panel.TLabel").pack(side="left")
            search_var = tk.StringVar()
            search_entry = ttk.Entry(controls, textvariable=search_var, width=40)
            search_entry.pack(side="left", padx=(6, 10))
            pretty_var = tk.BooleanVar(value=False)
            line_numbers_var = tk.BooleanVar(value=True)
            view_mode_var = tk.StringVar(value="raw")
            ttk.Checkbutton(
                controls,
                text="Show JSONL pretty",
                variable=pretty_var,
                style="Panel.TLabel",
            ).pack(side="left")
            ttk.Checkbutton(
                controls,
                text="Line numbers",
                variable=line_numbers_var,
                style="Panel.TLabel",
            ).pack(side="left", padx=(8, 0))
            ttk.Label(controls, text="View:", style="Panel.TLabel").pack(side="left", padx=(8, 4))
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

            def format_with_line_no(line_number: int, content: str) -> str:
                if line_numbers_var.get():
                    return f"{line_number:04d}: {content}"
                return content

            def copy_visible_text() -> None:
                current = preview_text.get("1.0", tk.END).rstrip("\n")
                if not current.strip():
                    messagebox.showwarning("Empty", "There is no content to copy.")
                    return
                preview.clipboard_clear()
                preview.clipboard_append(current)
                messagebox.showinfo("Done", "Visible content copied to clipboard.")

            ttk.Button(controls, text="Copy Visible", command=copy_visible_text).pack(
                side="right", padx=(6, 0)
            )

            def render_preview() -> None:
                preview_text.configure(state="normal")
                preview_text.delete("1.0", tk.END)
                if not lines:
                    preview_text.insert("1.0", "Archive file is empty.")
                    preview_text.configure(state="disabled")
                    return

                content_lines = lines[:max_lines]
                mode = view_mode_var.get().strip()
                rendered_lines: list[str] = []
                for line_number, line in enumerate(content_lines, start=1):
                    raw = format_with_line_no(line_number, line)
                    pretty = format_with_line_no(line_number, to_pretty(line))
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
                        f"\n\n... (total {len(lines)} lines, showing first {max_lines} only)",
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
                    match_start = preview_text.search(needle, start, stopindex=tk.END, nocase=True)
                    if not match_start:
                        break
                    match_end = f"{match_start}+{len(needle)}c"
                    preview_text.tag_add("match", match_start, match_end)
                    start = match_end

            search_entry.bind("<KeyRelease>", lambda _: highlight_search_matches())
            pretty_var.trace_add("write", lambda *_: render_preview())
            line_numbers_var.trace_add("write", lambda *_: render_preview())
            view_mode.bind("<<ComboboxSelected>>", lambda _: render_preview())
            render_preview()

        def export_selected_archive() -> None:
            selected = list(self.archive_listbox.curselection())
            if len(selected) != 1:
                messagebox.showwarning("Selection", "Select exactly one archive file for export.")
                return
            index = selected[0]
            if index < 0 or index >= len(archive_file_paths):
                messagebox.showerror("Error", "Selected archive file was not found.")
                return
            source = archive_file_paths[index]
            path = filedialog.asksaveasfilename(
                defaultextension=".jsonl",
                filetypes=[("JSONL", "*.jsonl"), ("All Files", "*.*")],
                initialfile=source.name,
                title="Export selected archive",
            )
            if not path:
                return
            try:
                Path(path).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
            except OSError as exc:
                messagebox.showerror("Error", f"Could not export archive: {exc}")
                return
            messagebox.showinfo("Done", f"Archive exported:\n{path}")

        buttons = ttk.Frame(frame, style="Panel.TFrame")
        buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(buttons, text="Run Housekeeping Now", command=run_housekeeping_now).pack(
            side="left"
        )
        ttk.Button(buttons, text="Export Report as TXT", command=lambda: export_health_report(False)).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Export Report as JSON", command=lambda: export_health_report(True)).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Open Archive Folder", command=open_archive_folder).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Export Selected Archive", command=export_selected_archive).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Preview Selected Archive", command=preview_selected_archive).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Delete Selected Archives", command=delete_selected_archives).pack(
            side="left", padx=(6, 0)
        )

        render_report()

    def show_history_dialog(self) -> None:
        """Show filterable operation history and CSV export tools."""
        entries = load_history(limit=500)
        dialog = tk.Toplevel(self.root)
        dialog.title("Operation History")
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

        ttk.Label(controls, text="Search:", style="Panel.TLabel").pack(side="left")
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
                text.insert("1.0", "No records matched the current filter.")
            else:
                for item in filtered_entries:
                    ts = item.get("timestamp", "-")
                    event = item.get("event", "-")
                    payload = item.get("payload", {})
                    text.insert(tk.END, f"[{ts}] {event}\n{payload}\n\n")
            text.configure(state="disabled")

        def export_filtered_csv() -> None:
            if not filtered_entries:
                messagebox.showwarning("Empty Result", "There are no history records to export.")
                return
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                title="Export history to CSV",
            )
            if not path:
                return
            with Path(path).open("w", newline="", encoding="utf-8-sig") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["timestamp", "event", "payload"])
                for item in filtered_entries:
                    writer.writerow([item.get("timestamp", ""), item.get("event", ""), item.get("payload", {})])
            messagebox.showinfo("Done", f"History CSV exported:\n{path}")

        def archive_current_history() -> None:
            archive_path = archive_history()
            if archive_path is None:
                messagebox.showinfo("Info", "No history records available for archiving.")
                return
            reload_entries()
            apply_filter()
            messagebox.showinfo("Done", f"History archived:\n{archive_path}")

        def rotate_old_history() -> None:
            kept, removed = rotate_history(days=self.prefs.history_retention_days)
            reload_entries()
            apply_filter()
            messagebox.showinfo(
                "Rotation Completed",
                (
                    f"Records older than {self.prefs.history_retention_days} days were removed.\n"
                    f"Kept: {kept}\nRemoved: {removed}"
                ),
            )

        def clear_state_files_action() -> None:
            extra_state = []
            if self.prefs.last_download_dir:
                extra_state.append(Path(self.prefs.last_download_dir) / DEFAULT_JOB_STATE_NAME)
            removed = clear_state_files(extra_paths=extra_state)
            if removed:
                removed_lines = "\n".join(str(item) for item in removed)
                messagebox.showinfo("Done", f"State files deleted:\n{removed_lines}")
            else:
                messagebox.showinfo("Info", "No state files found to delete.")

        buttons = ttk.Frame(root_frame, style="Panel.TFrame")
        buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(buttons, text="Apply Filter", command=apply_filter).pack(side="left")
        ttk.Button(buttons, text="Export Filter to CSV", command=export_filtered_csv).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Archive History", command=archive_current_history).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Rotate 30+ Days", command=rotate_old_history).pack(
            side="left", padx=(6, 0)
        )
        ttk.Button(buttons, text="Clear State Files", command=clear_state_files_action).pack(
            side="left", padx=(6, 0)
        )

        event_combo.bind("<<ComboboxSelected>>", lambda _: apply_filter())
        search_entry.bind("<KeyRelease>", lambda _: apply_filter())
        apply_filter()
