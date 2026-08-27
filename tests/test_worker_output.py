"""Worker output transformation invariants (pure and file-backed, no processes)."""

import json
import logging
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from lubko import worker
from lubko.protocol import OUTPUT_CHUNK_MAX_BYTES, OUTPUT_TAIL_MAX_BYTES
from lubko.worker import (
    OutputStream,
    align_code_point_end,
    align_code_point_start,
    archive_target,
    cleanup_job,
    output_window_text,
    pg_safe_decode,
    truncate_output,
)


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


def test_output_window_preserves_multibyte_rune_alignment(tmp_path: Path) -> None:
    """Window limits stay byte-bounded but never split a multi-byte rune."""
    path = tmp_path / "s"
    path.write_bytes("é".encode())  # two bytes
    # A 1-byte window cannot hold the rune, so the head aligns forward to the
    # rune boundary; the empty window stays strictly within the byte bound.
    head_text, start, end = output_window_text(path, 1)
    assert (start, end) == (2, 2)
    assert not head_text
    # A window that fits the rune keeps it intact with byte offsets.
    text, start, end = output_window_text(path, 2)
    assert (text, start, end) == ("é", 0, 2)


def test_cleanup_job_isolates_spool_removal_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed spool unlink cannot block sibling cleanup or escape."""
    stdout_path = tmp_path / "stdout"
    stderr_path = tmp_path / "stderr"
    stdout_path.write_bytes(b"out")
    stderr_path.write_bytes(b"err")
    real_unlink = Path.unlink
    attempted: list[Path] = []

    def unlink(path: Path, *, missing_ok: bool = False) -> None:
        attempted.append(path)
        if path == stdout_path:
            message = "synthetic spool cleanup failure"
            raise PermissionError(message)
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)

    def stream(path: Path) -> SimpleNamespace:
        return SimpleNamespace(fd=None, path=path)

    job = cast("Any", SimpleNamespace(stdout=stream(stdout_path), stderr=stream(stderr_path)))
    with caplog.at_level(logging.WARNING, logger="lubko.worker"):
        cleanup_job(job)

    assert attempted == [stdout_path, stderr_path]
    assert stdout_path.exists()
    assert not stderr_path.exists()
    assert "failed to remove capture spool" in caplog.text


def test_cleanup_all_files_continues_after_one_spool_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Shutdown cleanup keeps visiting later jobs after one unlink failure."""
    paths = [tmp_path / name for name in ("a-out", "a-err", "b-out", "b-err")]
    for path in paths:
        path.write_bytes(b"x")
    real_unlink = Path.unlink
    attempted: list[Path] = []

    def unlink(path: Path, *, missing_ok: bool = False) -> None:
        attempted.append(path)
        if path == paths[0]:
            message = "synthetic cleanup failure"
            raise OSError(message)
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)

    def job(stdout: Path, stderr: Path) -> SimpleNamespace:
        return SimpleNamespace(
            stdout=SimpleNamespace(fd=None, path=stdout),
            stderr=SimpleNamespace(fd=None, path=stderr),
        )

    supervisor = cast(
        "Any",
        SimpleNamespace(active={"a": job(paths[0], paths[1]), "b": job(paths[2], paths[3])}),
    )
    worker.Supervisor._cleanup_all_files(supervisor)

    assert attempted == paths
    assert paths[0].exists()
    assert all(not path.exists() for path in paths[1:])


def _mixed_runes(byte_len: int, seed: int) -> bytes:
    """Deterministic mixed 1/2/3/4-byte rune sequence of at least ``byte_len`` bytes.

    Returns:
        A byte string of complete runes (never cut mid-rune) of total length
        ``>= byte_len``, built from valid UTF-8 runes of widths 1, 2, 3 and 4.
    """
    table = [b"a", "é".encode(), "€".encode(), "𐍈".encode()]
    out = bytearray()
    i = seed
    while len(out) < byte_len:
        out += table[i % 4]
        i += 1
    return bytes(out)


def test_align_code_point_helpers_snap_to_code_point_edges() -> None:
    """Boundary helpers snap only across structurally valid code points."""
    data = "aé€𐍈".encode()
    n = len(data)
    leads = {i for i in range(n) if _is_lead(data[i])} | {n}
    for offset in range(n + 1):
        if offset in leads:
            # A lead/ASCII byte (or the end) is already a boundary.
            assert align_code_point_start(data, offset) == offset
            assert align_code_point_end(data, offset) == offset
        else:
            # A continuation byte inside a valid rune snaps forward (head) and
            # backward (end).
            assert align_code_point_start(data, offset) > offset
            assert align_code_point_end(data, offset) < offset
    # The result is always a valid boundary (a lead byte or an end).
    for offset in range(n + 1):
        s = align_code_point_start(data, offset)
        assert s == n or _is_lead(data[s])


def test_align_code_point_helpers_ignore_invalid_continuation_runs() -> None:
    """Invalid continuation runs are left on the raw boundary (no scanning)."""
    # A long run of bare continuation bytes is not a valid code point; the
    # boundary must stay exactly where requested rather than scanning.
    invalid = b"\x80" * 50
    for offset in range(len(invalid) + 1):
        assert align_code_point_start(invalid, offset) == offset
        assert align_code_point_end(invalid, offset) == offset
    # An orphan 2-byte lead with a missing continuation is also left unchanged.
    orphan = b"\xc3" + b"x" * 10
    assert align_code_point_end(orphan, 1) == 1
    assert align_code_point_start(orphan, 1) == 1


def _is_lead(byte: int) -> bool:
    return byte < 0x80 or byte >= 0xC0


def test_live_tail_never_splits_multibyte_rune(tmp_path: Path) -> None:
    """The live-tail window stays within its byte bound and keeps whole runes.

    A 4-byte rune is placed so the ``OUTPUT_TAIL_MAX_BYTES`` cut lands on its
    second byte; the window head must align forward to the next rune boundary,
    never emitting a partial rune replaced with U+FFFD.
    """
    path = tmp_path / "s"
    rune4 = "𐍈".encode()
    # rune4 starts at offset 3999 so the tail cut (size - 4000 = 4000) is its 2nd byte.
    content = b"x" * 3999 + rune4 + b"y" * 3997
    path.write_bytes(content)
    text, start, end = output_window_text(path, OUTPUT_TAIL_MAX_BYTES)
    # Head moved forward by at most three bytes to a code-point boundary.
    assert start >= len(content) - OUTPUT_TAIL_MAX_BYTES
    assert start - (len(content) - OUTPUT_TAIL_MAX_BYTES) <= 3
    assert start == align_code_point_start(content, len(content) - OUTPUT_TAIL_MAX_BYTES)
    # The tail is exactly the decode of its byte range and contains no garbage.
    assert text == content[start:end].decode("utf-8")
    assert "\ufffd" not in text


def test_archive_chunk_preserves_multibyte_rune_across_boundary(tmp_path: Path) -> None:
    """An immutable chunk ends on a code-point boundary; runes are not split.

    A 4-byte rune is placed so ``OUTPUT_CHUNK_MAX_BYTES`` falls on its second
    byte. The planned chunk must contain the whole rune in exactly one chunk and
    its decoded value must equal the exact decode of its byte range.
    """
    path = tmp_path / "s"
    rune4 = "𐍈".encode()
    content = (
        b"x" * (OUTPUT_CHUNK_MAX_BYTES - 1) + rune4 + rune4 + b"y" * (OUTPUT_TAIL_MAX_BYTES + 500)
    )
    path.write_bytes(content)
    stream = OutputStream(path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0)
    chunks, _archived, _last, _seq = worker._plan_chunks(
        uuid.UUID(int=1), "stdout", stream, len(content), "server"
    )
    assert chunks
    parsed_chunks = [json.loads(payload) for _cid, payload in chunks]
    for parsed in parsed_chunks:
        start, end, value = parsed["start"], parsed["end"], parsed["value"]
        # Faithful: the chunk value is the exact decode of its byte range, so a
        # rune straddling the boundary is intact rather than replaced.
        assert value == content[start:end].decode("utf-8")
    # The rune that straddles the raw chunk boundary is wholly inside one chunk.
    rune_start = OUTPUT_CHUNK_MAX_BYTES - 1
    rune_end = rune_start + 4
    owners = [p for p in parsed_chunks if p["start"] <= rune_start and p["end"] >= rune_end]
    assert len(owners) == 1
    # The rune is not split: its bytes live in exactly one immutable chunk.
    assert owners[0]["value"] == content[owners[0]["start"] : owners[0]["end"]].decode("utf-8")


def test_repeated_trim_publication_cycles_preserve_runes(tmp_path: Path) -> None:
    """Many append/plan/trim cycles keep every published byte range faithful.

    Across live-tail and archive-chunk boundaries, no 2/3/4-byte rune is ever
    split into U+FFFD: each emitted chunk value and the final tail equal the
    exact decode of their absolute byte range in the whole stream.
    """
    path = tmp_path / "stdout"
    path.write_bytes(b"")
    job = cast("Any", SimpleNamespace(id=uuid.UUID(int=0), stdout=OutputStream(path=path)))
    full = bytearray()
    published: list[tuple[int, int, str]] = []
    # Deliberately awkward per-cycle lengths so boundaries cross runes.
    for cycle in range(24):
        delta = _mixed_runes(900 + (cycle % 7) * 137, cycle)
        full += delta
        # Append the new tail to the spool file (it already holds [spool_start, ...]).
        with path.open("r+b") as fh:
            fh.seek(0, 2)
            fh.write(delta)
        plans = worker._plan_streams(job, ["stdout"], server="server", force=True)
        for name, plan in plans.items():
            for _cid, payload in plan.chunks:
                parsed = json.loads(payload)
                published.append((parsed["start"], parsed["end"], parsed["value"]))
            worker._apply_plan(getattr(job, name), plan, time.monotonic())
        worker._trim_published(job, plans)
    # Every chunk is the exact decode of its absolute byte range.
    for start, end, value in published:
        assert value == full[start:end].decode("utf-8")
    # The final live tail is the exact decode of its window.
    stream = job.stdout
    assert stream.tail_text == full[stream.tail_start : stream.tail_end].decode("utf-8")
    # Both boundaries are code-point aligned, so no valid rune was split.
    assert align_code_point_end(bytes(full), stream.tail_end) == stream.tail_end
    for start, end, _value in published:
        assert align_code_point_start(bytes(full), start) == start
        assert align_code_point_end(bytes(full), end) == end


def test_invalid_utf8_policy_is_deterministic_and_safe(tmp_path: Path) -> None:
    """Invalid bytes become U+FFFD deterministically; boundaries never raise.

    An invalid byte sequence (a lead with no continuation) is placed across both
    the archive-chunk and live-tail byte boundaries. Publication must never raise
    and must match the canonical ``pg_safe_decode`` of each byte range exactly.
    """
    path = tmp_path / "s"
    # b"\xc3" alone is an orphan 2-byte lead; padding puts it across boundaries.
    content = b"a" * (OUTPUT_CHUNK_MAX_BYTES - 1) + b"\xc3" + b"b" * (OUTPUT_TAIL_MAX_BYTES + 5)
    path.write_bytes(content)
    # The canonical conversion is deterministic and safe for PostgreSQL.
    assert pg_safe_decode(b"\xc3") == "\ufffd"
    # Live tail: no exception, exact canonical decode of its range.
    text, start, end = output_window_text(path, OUTPUT_TAIL_MAX_BYTES)
    assert text == pg_safe_decode(content[start:end])
    # Archive chunks: no exception, exact canonical decode of each range.
    stream = OutputStream(path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0)
    chunks, _archived, _last, _seq = worker._plan_chunks(
        uuid.UUID(int=2), "stdout", stream, len(content), "server"
    )
    for _cid, payload in chunks:
        parsed = json.loads(payload)
        assert parsed["value"] == pg_safe_decode(content[parsed["start"] : parsed["end"]])


def test_plan_chunks_makes_progress_through_invalid_continuation_run(tmp_path: Path) -> None:
    """Invalid continuation bytes spanning >1 chunk must still terminate/progress.

    A run of bare continuation bytes (``0x80``) is never a structurally valid code
    point, so the boundary must stay exactly on the raw ``OUTPUT_CHUNK_MAX_BYTES``
    grid: ``_plan_chunks`` must make positive progress every iteration, terminate,
    keep every chunk within the byte bound, and emit values equal to the
    deterministic ``pg_safe_decode`` of each byte range. This guards against the
    regression where a ``while`` scan walked the whole run and stalled.
    """
    path = tmp_path / "s"
    # Longer than one chunk on purpose; the invalid run spans multiple boundaries.
    content = b"\x80" * (OUTPUT_CHUNK_MAX_BYTES * 3 + 100)
    path.write_bytes(content)
    stream = OutputStream(path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0)
    chunks, archived_upto, _last, _seq = worker._plan_chunks(
        uuid.UUID(int=3), "stdout", stream, len(content), "server"
    )
    assert chunks  # positive progress: at least one chunk planned
    assert archived_upto > 0  # and the loop advanced, i.e. terminated
    prev_end = 0
    for _cid, payload in chunks:
        parsed = json.loads(payload)
        start, end, value = parsed["start"], parsed["end"], parsed["value"]
        # Every chunk stays within the byte bound and is non-empty (progress).
        assert 0 < end - start <= OUTPUT_CHUNK_MAX_BYTES
        # Chunks are contiguous with no gap, proving steady forward progress.
        assert start == prev_end
        prev_end = end
        # Deterministic invalid-byte handling is delegated to pg_safe_decode.
        assert value == pg_safe_decode(content[start:end])
    assert prev_end == archived_upto


def test_output_window_text_uses_bounded_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live-tail decoding must read only a bounded tail, not the whole spool."""
    path = tmp_path / "s"
    # The spool dwarfs the bounded tail so a whole-file read would be huge.
    path.write_bytes(b"x" * (OUTPUT_TAIL_MAX_BYTES + 5000))
    seen: list[int] = []
    real_read_range = worker.read_range

    def spy(p: Path, start: int, end: int) -> bytes:
        seen.append(end - start)
        return real_read_range(p, start, end)

    monkeypatch.setattr(worker, "read_range", spy)
    output_window_text(path, OUTPUT_TAIL_MAX_BYTES)
    assert seen, "read_range must be the read seam used"
    # At most the tail plus three lookahead bytes for code-point classification.
    assert max(seen) <= OUTPUT_TAIL_MAX_BYTES + 3
    # The entire spool is never materialized in a single read.
    assert max(seen) < OUTPUT_TAIL_MAX_BYTES + 5000


def test_plan_chunks_uses_bounded_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Archive planning must read only bounded neighborhoods, not the whole spool."""
    path = tmp_path / "s"
    content = b"x" * 100 + b"y" * (OUTPUT_CHUNK_MAX_BYTES * 3 + 100)
    path.write_bytes(content)
    seen: list[int] = []
    real_read_range = worker.read_range

    def spy(p: Path, start: int, end: int) -> bytes:
        seen.append(end - start)
        return real_read_range(p, start, end)

    monkeypatch.setattr(worker, "read_range", spy)
    stream = OutputStream(path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0)
    worker._plan_chunks(uuid.UUID(int=5), "stdout", stream, len(content), "server")
    assert seen, "read_range must be the read seam used"
    # No single read may exceed a chunk plus its small boundary lookaround.
    assert max(seen) <= OUTPUT_CHUNK_MAX_BYTES + 8
    # The whole spool is never materialized in a single read.
    assert max(seen) < len(content)


def test_semantically_invalid_sequences_do_not_move_boundaries() -> None:
    """Structurally shaped but illegal scalars stay on the raw boundary.

    ``E0 80 80`` (overlong), ``ED A0 80`` (surrogate), ``F0 80 80 80`` (overlong)
    and ``F4 90 80 80`` (> U+10FFFF) look like valid leads but decode to illegal
    scalars; boundary detection must treat them as invalid so they are not moved
    and ``pg_safe_decode`` alone defines their replacement.
    """
    cases = [b"\xe0\x80\x80", b"\xed\xa0\x80", b"\xf0\x80\x80\x80", b"\xf4\x90\x80\x80"]
    for seq in cases:
        data = b"a" * 5 + seq + b"b" * 5
        candidate = 5 + 1  # a continuation byte inside the invalid sequence
        assert align_code_point_end(data, candidate) == candidate
        assert align_code_point_start(data, candidate) == candidate


def test_semantically_invalid_sequences_stay_within_pg_safe_decode(tmp_path: Path) -> None:
    """Invalid sequences spanning boundaries match the canonical decode exactly."""
    cases = [b"\xe0\x80\x80", b"\xed\xa0\x80", b"\xf0\x80\x80\x80", b"\xf4\x90\x80\x80"]
    for seq in cases:
        path = tmp_path / "s"
        # Place the invalid sequence so a chunk boundary would otherwise cut it.
        content = b"x" * (OUTPUT_CHUNK_MAX_BYTES - 1) + seq + b"y" * (OUTPUT_TAIL_MAX_BYTES + 5)
        path.write_bytes(content)
        text, start, end = output_window_text(path, OUTPUT_TAIL_MAX_BYTES)
        assert text == pg_safe_decode(content[start:end])
        stream = OutputStream(
            path=path, spool_start=0, archived_upto=0, last_chunk=None, sequence=0
        )
        chunks, _archived, _last, _seq = worker._plan_chunks(
            uuid.UUID(int=7), "stdout", stream, len(content), "server"
        )
        for _cid, payload in chunks:
            parsed = json.loads(payload)
            assert parsed["value"] == pg_safe_decode(content[parsed["start"] : parsed["end"]])
