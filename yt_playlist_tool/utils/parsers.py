"""User-input parsing and text extraction helpers."""

from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import parse_qs, urlparse

from yt_playlist_tool.utils.helpers import normalize_text

URL_PATTERN = re.compile(r"(https?://[^\s<>\"]+)", re.IGNORECASE)


def extract_playlist_id(text: str) -> str | None:
    """Extract a playlist ID from plain text, mixed text, or a URL."""
    raw_value = text.strip()
    if not raw_value:
        return None

    # Try URLs first, even when the input contains extra text around a URL.
    for url_candidate in URL_PATTERN.findall(raw_value):
        parsed = urlparse(url_candidate)
        query = parse_qs(parsed.query)
        candidate = query.get("list", [None])[0]
        if candidate:
            return candidate.strip()

    if raw_value.startswith("http://") or raw_value.startswith("https://"):
        parsed = urlparse(raw_value)
        query = parse_qs(parsed.query)
        candidate = query.get("list", [None])[0]
        if candidate:
            return candidate.strip()
        return None

    # Fall back to plain ID extraction from mixed text lines.
    id_match = re.search(r"\b(PL|UU|LL|OLAK5uy_)[A-Za-z0-9_-]{10,}\b", raw_value)
    if id_match:
        return id_match.group(0)

    cleaned = raw_value.strip(" ,;\"'()[]{}")
    return cleaned or None


def parse_playlist_id_list(raw: str) -> list[str]:
    """Return unique playlist IDs from line- or comma-separated input."""
    playlist_ids: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[\n,]+", raw):
        playlist_id = extract_playlist_id(part)
        if playlist_id and playlist_id not in seen:
            seen.add(playlist_id)
            playlist_ids.append(playlist_id)
    return playlist_ids


def parse_range_string(range_str: str, max_index: int) -> list[int]:
    """Convert `1-3, 8, 10-12` style input into an index list."""
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
                raise ValueError(f"Invalid range: '{token}'")
            try:
                start = int(bounds[0].strip())
                end = int(bounds[1].strip())
            except ValueError as exc:
                raise ValueError(f"Invalid range: '{token}'") from exc
            if start > end:
                raise ValueError(f"Range start is greater than end: '{token}'")
            for index_value in range(start, end + 1):
                if 1 <= index_value <= max_index:
                    selected_indices.add(index_value)
        else:
            try:
                index_value = int(token)
            except ValueError as exc:
                raise ValueError(f"Invalid index: '{token}'") from exc
            if 1 <= index_value <= max_index:
                selected_indices.add(index_value)

    if not selected_indices:
        raise ValueError("No valid indices were produced. Please check your range input.")
    return sorted(selected_indices)


def extract_pdf_links_from_text(text: str) -> list[str]:
    """Extract PDF and Google Drive links from text."""
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
    """Convert a Google Drive share link to a direct download URL."""
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
    """Normalize and return search tokens from free-form text."""
    parts = re.split(r"[,\s]+", search_text.strip())
    return [normalize_text(p) for p in parts if p.strip()]


def title_matches_terms(title: str, terms: Iterable[str]) -> bool:
    """Return True if normalized title contains all search terms."""
    term_list = list(terms)
    if not term_list:
        return True
    normalized = normalize_text(title)
    return all(term in normalized for term in term_list)


def tokenize_for_topic(text: str) -> list[str]:
    """Split a filename or title into topic-analysis tokens."""
    text = normalize_text(text)
    chunks = re.split(r"[_\-\s\.]+", text)
    return [c for c in chunks if c and not c.isdigit() and len(c) > 1]
