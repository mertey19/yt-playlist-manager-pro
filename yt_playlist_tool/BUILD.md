# Build ve Test Rehberi

## 1) Kurulum

```powershell
python -m pip install -r yt_playlist_tool/requirements-dev.txt
```

## 2) Unit Test Çalıştırma

```powershell
python -m pytest yt_playlist_tool/tests -q
```

## 3) EXE Build (PyInstaller)

PowerShell'de proje kökünden:

```powershell
python -m PyInstaller --clean --noconfirm --distpath dist --workpath build "yt_playlist_tool/pyinstaller/yt_playlist_tool.spec"
```

Çıktı:

- `dist/yt_playlist_tool.exe`

## 4) Çalışma Zamanı Dosyaları

Uygulama aşağıdaki dosyaları çalışma klasöründe bekler:

- `client_secret.json`
- (ilk login sonrası oluşur) `token.pickle`

Not:

- OAuth doğrulama ilk çalıştırmada tarayıcı açar.
- Uygulama log ve preferences dosyalarını kullanıcı home altında `.yt_playlist_tool` klasörüne yazar.
