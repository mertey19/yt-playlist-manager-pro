"""Unit tests for parser and validator utilities."""

from __future__ import annotations

import pytest

from yt_playlist_tool.utils.parsers import (
    build_search_terms,
    convert_drive_link_to_direct,
    extract_pdf_links_from_text,
    extract_playlist_id,
    parse_playlist_id_list,
    parse_range_string,
    title_matches_terms,
)


def test_extract_playlist_id_from_url() -> None:
    url = "https://www.youtube.com/playlist?list=PL1234567890ABCDE"
    assert extract_playlist_id(url) == "PL1234567890ABCDE"


def test_extract_playlist_id_from_mixed_text_url() -> None:
    text = "Please check this playlist: https://www.youtube.com/playlist?list=PLXYZ1234567890ABCDE&si=abc"
    assert extract_playlist_id(text) == "PLXYZ1234567890ABCDE"


def test_extract_playlist_id_from_mixed_text_plain_id() -> None:
    text = "source -> PL1234567890ABCDE extra"
    assert extract_playlist_id(text) == "PL1234567890ABCDE"


def test_extract_playlist_id_from_raw_id() -> None:
    assert extract_playlist_id("PL_TEST_001") == "PL_TEST_001"


def test_parse_playlist_id_list_deduplicates() -> None:
    raw = "PL_A,\nhttps://youtube.com/playlist?list=PL_A\nPL_B"
    assert parse_playlist_id_list(raw) == ["PL_A", "PL_B"]


def test_parse_range_string_valid() -> None:
    assert parse_range_string("1-3, 5, 7-8", max_index=10) == [1, 2, 3, 5, 7, 8]


def test_parse_range_string_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        parse_range_string("x-4", max_index=10)


def test_drive_link_conversion_file_d() -> None:
    shared = "https://drive.google.com/file/d/ABC123/view?usp=sharing"
    assert convert_drive_link_to_direct(shared) == (
        "https://drive.google.com/uc?export=download&id=ABC123"
    )


def test_extract_pdf_links_from_text() -> None:
    text = (
        "lecture note: https://example.com/a.pdf "
        "and drive: https://drive.google.com/file/d/XYZ/view?usp=sharing"
    )
    links = extract_pdf_links_from_text(text)
    assert len(links) == 2


def test_title_matches_terms_normalized_matching() -> None:
    terms = build_search_terms("turev integral")
    assert title_matches_terms("AYT TUREV and Integral camp", terms)
