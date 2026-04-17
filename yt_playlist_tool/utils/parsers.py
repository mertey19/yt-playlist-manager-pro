"""Parsers and validators for user input and text extraction."""

from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import parse_qs, urlparse

from yt_playlist_tool.utils.helpers import normalize_text

URL_PATTERN = re.compile(r"(https?://[^\s<>\"]+)", re.IGNORECASE)


def extract_playlist_id(text: str) -> str | None:
    """Extract playlist ID from raw ID or URL input."""
    text = text.strip()
    if not text:
        return None

    if text.startswith("http://") or text.startswith("https://"):
        parsed = urlparse(text)
        query = parse_qs(parsed.query)
        candidate = query.get("list", [None])[0]
        return candidate.strip() if candidate else None

    return text


def parse_playlist_id_list(raw: str) -> list[str]:
    """Parse multiline/comma-separated playlist inputs without duplicates."""
    ids: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[\n,]+", raw):
        pid = extract_playlist_id(part)
        if pid and pid not in seen:
            seen.add(pid)
            ids.append(pid)
    return ids


def parse_range_string(range_str: str, max_index: int) -> list[int]:
    """Parse index range strings like '1-3, 8, 10-12'."""
    indices: set[int] = set()
    raw = range_str.strip()
    if not raw:
        return []

    for part in raw.split(","):
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
            for i in range(start, end + 1):
                if 1 <= i <= max_index:
                    indices.add(i)
        else:
            try:
                i = int(token)
            except ValueError as exc:
                raise ValueError(f"Geçersiz index: '{token}'") from exc
            if 1 <= i <= max_index:
                indices.add(i)

    if not indices:
        raise ValueError("Hiç geçerli index üretilmedi, aralıkları kontrol edin.")
    return sorted(indices)


def extract_pdf_links_from_text(text: str) -> list[str]:
    """Extract probable PDF and Google Drive URLs from plain text."""
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
    """Convert common Drive share URLs to direct download format."""
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
    """Split and normalize search terms from free text."""
    parts = re.split(r"[,\s]+", search_text.strip())
    return [normalize_text(p) for p in parts if p.strip()]


def title_matches_terms(title: str, terms: Iterable[str]) -> bool:
    """Return True when all normalized terms appear in normalized title."""
    terms = list(terms)
    if not terms:
        return True
    normalized = normalize_text(title)
    return all(term in normalized for term in terms)


def tokenize_for_topic(text: str) -> list[str]:
    """Tokenize a filename/title into meaningful normalized words."""
    text = normalize_text(text)
    chunks = re.split(r"[_\-\s\.]+", text)
    return [c for c in chunks if c and not c.isdigit() and len(c) > 1]
