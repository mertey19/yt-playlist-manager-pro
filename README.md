# yt-playlist-manager-pro

Tkinter tabanlı, üretim yaklaşımıyla düzenlenmiş bir **YouTube playlist yönetim ve PDF toplama** masaüstü uygulaması.

## Neler Yapıyor?

- Google YouTube API ile bağlanır
- Bir veya birden çok playlist’ten videoları çeker
- Başlığa göre filtreleme yapar
- Seçilen veya aralıkla belirtilen videoları hedef playlist’e ekler
- Hedef playlist’teki duplikeleri atlar
- Video açıklamalarından PDF / Google Drive linklerini bulur
- PDF dosyalarını indirip konuya göre ZIP içine yerleştirir
- Uzun işlemlerde iptal (cancel), progress ve durum yönetimi sunar
- İşlem geçmişi, bakım (maintenance), arşivleme ve rapor dışa aktarma özellikleri içerir

## Proje Yapısı

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

## Kurulum

### 1) Gereksinimleri yükle

```powershell
python -m pip install -r yt_playlist_tool/requirements.txt
```

Geliştirme ve test araçları için:

```powershell
python -m pip install -r yt_playlist_tool/requirements-dev.txt
```

### 2) OAuth dosyalarını hazırla

Çalıştırmadan önce proje kökünde şu dosya bulunmalı:

- `client_secret.json`

İlk girişten sonra `token.pickle` otomatik oluşur.

## Uygulamayı Çalıştırma

```powershell
python main.py
```

Alternatif:

```powershell
python yt_playlist_tool.py
```

## Test Çalıştırma

```powershell
python -m pytest yt_playlist_tool/tests -q
```

## EXE Build (PyInstaller)

```powershell
python -m PyInstaller --clean --noconfirm --distpath dist --workpath build "yt_playlist_tool/pyinstaller/yt_playlist_tool.spec"
```

Çıktı:

- `dist/yt_playlist_tool.exe`

## Öne Çıkan Teknik Özellikler

- Modüler mimari (UI / services / utils ayrımı)
- Retry + backoff + throttle destekli ağ katmanı
- Resumable job akışı (PDF ve transfer state)
- Thread-safe UI güncellemeleri (queue + main thread)
- Ayarlanabilir timeout/retry/backoff/throttle
- Gelişmiş bakım ekranı:
  - startup housekeeping
  - history rotate
  - weekly archive
  - archive max file limiti
  - maintenance raporu (TXT/JSON export)
- Arşiv önizleme:
  - arama
  - satır numarası
  - raw / pretty / compare görünüm
  - panoya kopyalama

## Notlar

- Uygulama log, ayar ve geçmiş dosyalarını kullanıcı profilindeki `.yt_playlist_tool` klasöründe tutar.
- YouTube API kota veya ağ sorunlarında kullanıcıya anlaşılır hata çıktısı vermek için kontrollü exception handling uygulanmıştır.

