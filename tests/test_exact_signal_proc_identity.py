"""Deterministic tests for the shared _exact_signal process-identity boundary.

All tests inject synthetic /proc/<pid>/stat data via _read_proc_stat so they
are fast, hermetic, and do not depend on real PIDs or sleeps.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from lubko import _exact_signal  # ruff: ignore[import-private-name]
from lubko._exact_signal import (  # ruff: ignore[import-private-name]
    STAT_MIN_FIELDS,
    STAT_PGRP_FIELD_INDEX,
    STAT_STARTTIME_FIELD_INDEX,
    STAT_STATE_FIELD_INDEX,
    _read_proc_stat,
    _split_stat_fields,
    proc_cpu_seconds,
    proc_start_ticks,
    process_is_zombie,
    process_pgrp,
    process_ppid,
    process_state_char,
)


def _build_stat(  # ruff: ignore[too-many-arguments]
    *,
    state: str = "S",
    ppid: int = 1,
    pgrp: int = 100,
    starttime: int = 5000,
    utime: int = 100,
    stime: int = 50,
) -> bytes:
    """Build a synthetic /proc/<pid>/stat byte string with 22+ fields.

    Returns:
        A realistic stat byte string parseable by _split_stat_fields.
    """
    comm = "(test)"
    fields: list[bytes] = [
        state.encode(),
        str(ppid).encode(),
        str(pgrp).encode(),
        b"0",  # session
        b"0",  # tty_nr
        b"0",  # tpgid
        b"0",  # flags
        b"0",  # minflt
        b"0",  # cminflt
        b"0",  # majflt
        b"0",  # cmajflt
        str(utime).encode(),
        str(stime).encode(),
        b"0",  # cutime
        b"0",  # cstime
        b"0",  # priority
        b"0",  # nice
        b"0",  # num_threads
        b"0",  # itrealvalue
        str(starttime).encode(),
    ]
    while len(fields) < STAT_MIN_FIELDS:
        fields.append(b"0")
    return comm.encode() + b" " + b" ".join(fields)


# -- _split_stat_fields ----------------------------------------------------


def test_split_valid_stat() -> None:
    """Valid stat line is split into correct fields."""
    raw = _build_stat(state="S", pgrp=42, starttime=999)
    fields = _split_stat_fields(raw)
    assert fields is not None
    assert fields[STAT_STATE_FIELD_INDEX] == b"S"
    assert int(fields[STAT_PGRP_FIELD_INDEX]) == 42
    assert int(fields[STAT_STARTTIME_FIELD_INDEX]) == 999


def test_split_comm_with_spaces_and_parens() -> None:
    """Comm field containing spaces and parens is skipped correctly."""
    raw = b"(a (b) c) S 1 2 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
    fields = _split_stat_fields(raw)
    assert fields is not None
    assert fields[STAT_STATE_FIELD_INDEX] == b"S"


def test_split_no_closing_paren() -> None:
    """Missing closing paren returns None."""
    assert _split_stat_fields(b"(broken S 1 2 3") is None


def test_split_too_few_fields() -> None:
    """Fewer than STAT_MIN_FIELDS returns None."""
    assert _split_stat_fields(b"(x) S 1 2") is None


# -- _read_proc_stat -------------------------------------------------------


def test_read_proc_stat_missing_pid() -> None:
    """Non-existent PID returns None."""
    assert _read_proc_stat(999999999) is None


def test_read_proc_stat_live_pid() -> None:
    """Current process returns stat bytes."""
    result = _read_proc_stat(os.getpid())
    assert result is not None
    assert b")" in result


# -- proc_start_ticks ------------------------------------------------------


def test_proc_start_ticks_valid() -> None:
    """Valid stat returns correct start ticks."""
    fake = _build_stat(starttime=7777)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert proc_start_ticks(1) == 7777


def test_proc_start_ticks_unreadable() -> None:
    """Unreadable /proc returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=None):
        assert proc_start_ticks(1) is None


def test_proc_start_ticks_malformed() -> None:
    """Malformed stat bytes return None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=b"garbage"):
        assert proc_start_ticks(1) is None


def test_proc_start_ticks_non_numeric() -> None:
    """Non-numeric starttime returns None."""
    fields = [b"0"] * STAT_MIN_FIELDS
    fields[STAT_STATE_FIELD_INDEX] = b"S"
    fields[STAT_STARTTIME_FIELD_INDEX] = b"not_a_number"
    fake = b"(t) " + b" ".join(fields)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert proc_start_ticks(1) is None


# -- process_state_char ----------------------------------------------------


def test_process_state_char_running() -> None:
    """Running state is returned as character."""
    fake = _build_stat(state="R")
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_state_char(1) == "R"


def test_process_state_char_sleeping() -> None:
    """Sleeping state is returned as character."""
    fake = _build_stat(state="S")
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_state_char(1) == "S"


def test_process_state_char_zombie() -> None:
    """Zombie state is returned as character."""
    fake = _build_stat(state="Z")
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_state_char(1) == "Z"


def test_process_state_char_unreadable() -> None:
    """Unreadable /proc returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=None):
        assert process_state_char(1) is None


def test_process_state_char_malformed() -> None:
    """Malformed stat returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=b"bad"):
        assert process_state_char(1) is None


# -- process_is_zombie -----------------------------------------------------


def test_process_is_zombie_live() -> None:
    """Live process is not zombie."""
    fake = _build_stat(state="S")
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_is_zombie(1) is False


def test_process_is_zombie_zombie() -> None:
    """Zombie state returns True."""
    fake = _build_stat(state="Z")
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_is_zombie(1) is True


def test_process_is_zombie_dead() -> None:
    """Dead state returns True."""
    fake = _build_stat(state="X")
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_is_zombie(1) is True


def test_process_is_zombie_unreadable_fail_closed() -> None:
    """Unreadable /proc is treated as zombie (fail-closed)."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=None):
        assert process_is_zombie(1) is True


def test_process_is_zombie_malformed_fail_closed() -> None:
    """Malformed stat is treated as zombie (fail-closed)."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=b"bad"):
        assert process_is_zombie(1) is True


# -- process_ppid ----------------------------------------------------------


def test_process_ppid_valid() -> None:
    """Valid stat returns correct parent PID."""
    fake = _build_stat(ppid=42)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_ppid(1) == 42


def test_process_ppid_unreadable() -> None:
    """Unreadable /proc returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=None):
        assert process_ppid(1) is None


def test_process_ppid_malformed() -> None:
    """Malformed stat returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=b"bad"):
        assert process_ppid(1) is None


# -- proc_cpu_seconds ------------------------------------------------------


def test_proc_cpu_seconds_valid() -> None:
    """Valid stat returns positive CPU seconds."""
    fake = _build_stat(utime=100, stime=50)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        result = proc_cpu_seconds(1)
        assert result is not None
        assert result > 0


def test_proc_cpu_seconds_unreadable() -> None:
    """Unreadable /proc returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=None):
        assert proc_cpu_seconds(1) is None


def test_proc_cpu_seconds_malformed() -> None:
    """Malformed stat returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=b"bad"):
        assert proc_cpu_seconds(1) is None


# -- process_pgrp: single-snapshot invariant --------------------------------


def test_process_pgrp_valid() -> None:
    """Valid stat returns correct process group."""
    fake = _build_stat(state="S", pgrp=42)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_pgrp(1) == 42


def test_process_pgrp_zombie() -> None:
    """Zombie state returns None."""
    fake = _build_stat(state="Z", pgrp=42)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_pgrp(1) is None


def test_process_pgrp_dead_state() -> None:
    """Dead state returns None."""
    fake = _build_stat(state="X", pgrp=42)
    with patch.object(_exact_signal, "_read_proc_stat", return_value=fake):
        assert process_pgrp(1) is None


def test_process_pgrp_unreadable() -> None:
    """Unreadable /proc returns None."""
    with patch.object(_exact_signal, "_read_proc_stat", return_value=None):
        assert process_pgrp(1) is None


def test_process_pgrp_single_snapshot_no_double_read() -> None:
    """process_pgrp reads /proc exactly once.

    State and pgrp must come from the same snapshot so PID exit/reuse
    between observations cannot mix processes.
    """
    call_count = 0
    original = _exact_signal._read_proc_stat

    def counting_read(pid: int) -> bytes | None:
        nonlocal call_count
        call_count += 1
        return original(pid)

    with patch.object(_exact_signal, "_read_proc_stat", counting_read):
        process_pgrp(os.getpid())
    assert call_count == 1


def test_process_pgrp_single_snapshot_injected() -> None:
    """With injected data, pgrp and state are derived from one parse."""
    fake = _build_stat(state="S", pgrp=99)
    read_count = 0

    def once(_pid: int) -> bytes | None:
        nonlocal read_count
        read_count += 1
        return fake

    with patch.object(_exact_signal, "_read_proc_stat", once):
        assert process_pgrp(1) == 99
    assert read_count == 1
