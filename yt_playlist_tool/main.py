"""Application entrypoint."""

from __future__ import annotations

import tkinter as tk

from yt_playlist_tool.ui.app import PlaylistApp


def main() -> None:
    """Start Tkinter mainloop."""
    root = tk.Tk()
    PlaylistApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
