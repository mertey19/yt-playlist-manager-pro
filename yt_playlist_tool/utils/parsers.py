"""Kullanıcı girdisi çözümleme ve metin ayıklama yardımcıları."""

from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import parse_qs, urlparse

from yt_playlist_tool.utils.helpers import normalize_text

URL_PATTERN = re.compile(r"(https?://[^\s<>\"]+)", re.IGNORECASE)


def extract_playlist_id(text: str) -> str | None:
    """Playlist kimliğini düz metinden veya URL'den çıkarır."""
    raw_value = text.strip()
    if not raw_value:
        return None

    if raw_value.startswith("http://") or raw_value.startswith("https://"):
        parsed = urlparse(raw_value)
        query = parse_qs(parsed.query)
        candidate = query.get("list", [None])[0]
        return candidate.strip() if candidate else None

    return raw_value


def parse_playlist_id_list(raw: str) -> list[str]:
    """Satır veya virgülle ayrılmış playlist girişlerini tekilleştirerek döndürür."""
    playlist_ids: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[\n,]+", raw):
        playlist_id = extract_playlist_id(part)
        if playlist_id and playlist_id not in seen:
            seen.add(playlist_id)
            playlist_ids.append(playlist_id)
    return playlist_ids


def parse_range_string(range_str: str, max_index: int) -> list[int]:
    """`1-3, 8, 10-12` biçimindeki aralık metnini index listesine çevirir."""
    selected_indices: set[int] = set()
    cleaned = range_str.strip()
    if not cleaned:
        return []

    for part in cleaned.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            bounds = token.split("-", maxsplit=1)
            if len(bounds) != 2:
                raise ValueError(f"Geçersiz aralık: '{token}'")
            try:
                start = int(bounds[0].strip())
                end = int(bounds[1].strip())
            except ValueError as exc:
                raise ValueError(f"Geçersiz aralık: '{token}'") from exc
            if start > end:
                raise ValueError(f"Aralık başlangıcı bitişten büyük: '{token}'")
            for index_value in range(start, end + 1):
                if 1 <= index_value <= max_index:
                    selected_indices.add(index_value)
        else:
            try:
                index_value = int(token)
            except ValueError as exc:
                raise ValueError(f"Geçersiz index: '{token}'") from exc
            if 1 <= index_value <= max_index:
                selected_indices.add(index_value)

    if not selected_indices:
        raise ValueError("Hiç geçerli index üretilmedi, aralıkları kontrol edin.")
    return sorted(selected_indices)


def extract_pdf_links_from_text(text: str) -> list[str]:
    """Metin içindeki PDF ve Google Drive bağlantılarını çıkarır."""
    if not text:
        return []

    links: list[str] = []
    seen: set[str] = set()
    for url in URL_PATTERN.findall(text):
        clean_url = url.strip(")];,'\"")
        lower = clean_url.lower()
        if ".pdf" in lower or "drive.google.com" in lower:
            if clean_url not in seen:
                seen.add(clean_url)
                links.append(clean_url)
    return links


def convert_drive_link_to_direct(url: str) -> str:
    """Google Drive paylaşım linkini doğrudan indirme formatına çevirir."""
    parsed = urlparse(url)
    if "drive.google.com" not in parsed.netloc:
        return url

    query = parse_qs(parsed.query)
    file_id = None

    file_match = re.search(r"/file/d/([^/]+)", parsed.path)
    if file_match:
        file_id = file_match.group(1)
    elif "id" in query and query["id"]:
        file_id = query["id"][0]
    elif parsed.path.startswith("/uc") and "id" in query and query["id"]:
        file_id = query["id"][0]

    if not file_id:
        return url
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def build_search_terms(search_text: str) -> list[str]:
    """Serbest metindeki arama kelimelerini normalize ederek döndürür."""
    parts = re.split(r"[,\s]+", search_text.strip())
    return [normalize_text(p) for p in parts if p.strip()]


def title_matches_terms(title: str, terms: Iterable[str]) -> bool:
    """Başlıktaki normalize metin tüm arama kelimelerini içeriyorsa True döner."""
    term_list = list(terms)
    if not term_list:
        return True
    normalized = normalize_text(title)
    return all(term in normalized for term in term_list)


def tokenize_for_topic(text: str) -> list[str]:
    """Dosya adı veya başlığı konu analizi için kelimelere ayırır."""
    text = normalize_text(text)
    chunks = re.split(r"[_\-\s\.]+", text)
    return [c for c in chunks if c and not c.isdigit() and len(c) > 1]
