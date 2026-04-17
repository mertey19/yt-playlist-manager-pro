# Build and Test Guide

## 1) Setup

```powershell
python -m pip install -r yt_playlist_tool/requirements-dev.txt
```

## 2) Run Unit Tests

```powershell
python -m pytest yt_playlist_tool/tests -q
```

## 3) Build EXE (PyInstaller)

Run this from the project root in PowerShell:

```powershell
python -m PyInstaller --clean --noconfirm --distpath dist --workpath build "yt_playlist_tool/pyinstaller/yt_playlist_tool.spec"
```

Output:

- `dist/yt_playlist_tool.exe`

## 4) Runtime Files

The application expects these files in the working directory:

- `client_secret.json`
- (created after first login) `token.pickle`

Notes:

- OAuth authentication opens a browser on first run.
- The app writes log and preferences files under `.yt_playlist_tool` in the user home directory.
