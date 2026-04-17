# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for yt_playlist_tool."""

from PyInstaller.utils.hooks import collect_submodules

hidden_imports = []
hidden_imports += collect_submodules("googleapiclient")
hidden_imports += collect_submodules("google_auth_oauthlib")
hidden_imports += collect_submodules("google.auth")

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='yt_playlist_tool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
