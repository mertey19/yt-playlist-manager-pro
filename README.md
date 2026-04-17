# yt-playlist-manager-pro

A Tkinter desktop application for **YouTube playlist management and PDF collection**, organized with a production-oriented architecture.

## What It Does

- Connects to the Google YouTube API
- Fetches videos from one or more playlists
- Filters videos by title
- Adds selected videos (or ranged selections) to a target playlist
- Skips duplicates already present in the target playlist
- Extracts PDF / Google Drive links from video descriptions
- Downloads PDFs and places them into topic-based folders in a ZIP archive
- Provides cancellation, progress tracking, and status updates for long operations
- Includes operation history, maintenance tools, archiving, and export features

## Project Structure

```text
.
├─ main.py
├─ yt_playlist_tool.py
├─ README.md
└─ yt_playlist_tool/
   ├─ config.py
   ├─ requirements.txt
   ├─ requirements-dev.txt
   ├─ BUILD.md
   ├─ services/
   │  ├─ youtube_service.py
   │  └─ pdf_service.py
   ├─ ui/
   │  └─ app.py
   ├─ utils/
   │  ├─ helpers.py
   │  └─ parsers.py
   ├─ tests/
   │  ├─ test_parsers.py
   │  ├─ test_pdf_service.py
   │  └─ test_helpers_history.py
   └─ pyinstaller/
      └─ yt_playlist_tool.spec
```

## Setup

### 1) Install dependencies

```powershell
python -m pip install -r yt_playlist_tool/requirements.txt
```

For development and test tools:

```powershell
python -m pip install -r yt_playlist_tool/requirements-dev.txt
```

### 2) Prepare OAuth files

Before running the app, make sure this file exists in the project root:

- `client_secret.json`

After the first login, `token.pickle` is created automatically.

## Run the Application

```powershell
python main.py
```

Alternative:

```powershell
python yt_playlist_tool.py
```

## Run Tests

```powershell
python -m pytest yt_playlist_tool/tests -q
```

## EXE Build (PyInstaller)

```powershell
python -m PyInstaller --clean --noconfirm --distpath dist --workpath build "yt_playlist_tool/pyinstaller/yt_playlist_tool.spec"
```

Output:

- `dist/yt_playlist_tool.exe`

## Key Technical Features

- Modular architecture (separated UI / services / utils layers)
- Network layer with retry + backoff + throttle support
- Resumable job flow (PDF and transfer state)
- Thread-safe UI updates (queue + main thread)
- Configurable timeout/retry/backoff/throttle
- Advanced maintenance panel:
  - startup housekeeping
  - history rotate
  - weekly archive
  - archive max file limit
  - maintenance report export (TXT/JSON)
- Archive preview:
  - search
  - line numbers
  - raw / pretty / compare views
  - copy visible content to clipboard

## Notes

- The app stores logs, settings, and history files under `.yt_playlist_tool` in the user profile directory.
- Controlled exception handling is used to provide clear feedback for API quota and network-related failures.

