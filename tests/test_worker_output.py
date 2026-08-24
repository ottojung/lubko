"""Worker output transformation invariants (pure and file-backed, no processes)."""

from pathlib import Path

import pytest

from lubko import worker
from lubko.worker import archive_target, output_window_text, pg_safe_decode, truncate_output


def test_pg_safe_decode_replaces_nul_and_invalid_bytes() -> None:
    """NUL and invalid UTF-8 become U+FFFD for PostgreSQL safety."""
    assert pg_safe_decode(b"ab\x00c\xff") == "ab\ufffdc\ufffd"


def test_truncate_output_bounds_and_marker() -> None:
    """Truncated output keeps the newest bytes under a hard bound."""
    marker = worker.TRUNCATION_MARKER
    data = b"x" * 100
    assert truncate_output(data, 200) == "x" * 100
    result = truncate_output(data, 50)
    assert len(result.encode()) <= 50
    assert result.startswith(marker.decode())
    # Newest bytes are retained.
    assert result.endswith("x" * (50 - len(marker)))
    with pytest.raises(ValueError, match="at least as large as the truncation marker"):
        truncate_output(data, len(marker) - 1)


def test_truncate_output_survives_multibyte_expansion() -> None:
    """Decoding expansion cannot push encoded output past the limit."""
    marker = len(worker.TRUNCATION_MARKER)
    # Each invalid byte becomes a 3-byte U+FFFD (100 raw bytes -> ~300
    # characters), so naive byte truncation would exceed the limit; the hard
    # bound must still hold.
    result = truncate_output(b"\xff" * 100, marker + 20)
    assert len(result.encode()) <= marker + 20


def test_archive_target_never_shortens_the_live_tail() -> None:
    """Archiving stays below or within the live tail window."""
    assert archive_target(0) == 0
    assert archive_target(500) == 0
    target = archive_target(10**9)
    assert 0 < target < 10**9


def test_output_window_text_returns_logical_offsets(tmp_path: Path) -> None:
    """Live windows return the newest bytes with logical offsets."""
    path = tmp_path / "stdout"
    path.write_bytes(b"abcdefgh")
    text, start, end = output_window_text(path, 4)
    assert (text, start, end) == ("efgh", 4, 8)
    text, start, end = output_window_text(path, 100)
    assert (text, start, end) == ("abcdefgh", 0, 8)
    # ``base`` translates physical file offsets to logical stream offsets.
    _text, start, end = output_window_text(path, 4, base=100)
    assert (start, end) == (104, 108)


def test_output_window_offsets_are_byte_based(tmp_path: Path) -> None:
    """Window limits count bytes even inside multi-byte runes."""
    path = tmp_path / "s"
    path.write_bytes("é".encode())  # two bytes
    _text, start, end = output_window_text(path, 1)
    # The limit is in bytes even when the cut lands inside a multi-byte rune.
    assert (start, end) == (1, 2)
    text, start, end = output_window_text(path, 2)
    assert (text, start, end) == ("é", 0, 2)
