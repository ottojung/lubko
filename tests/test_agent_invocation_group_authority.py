"""Fail-closed regressions for durable managed-agent invocation group authority."""

from __future__ import annotations

from typing import Any

import pytest

from lubko import agent


BASE_META: agent.Meta = {
    "id": "ab12",
    "pid": 4242,
    "pgid": 4242,
    "start_time": 1234,
    "invocation_id": "inv1",
}


@pytest.mark.parametrize("field", ["pid", "pgid"])
@pytest.mark.parametrize("value", [4242.9, "4242", True, 0, -1])
def test_group_signal_rejects_malformed_numeric_authority(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    """Malformed durable PID/PGID values never reach pinning or member scans."""
    meta = dict(BASE_META)
    meta[field] = value
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        agent,
        "open_pidfd",
        lambda pid: calls.append(("pin", pid)) or None,
    )
    monkeypatch.setattr(
        agent,
        "_pinned_invocation_members",
        lambda pgid, _aid, _iid: calls.append(("scan", pgid)) or [],
    )

    agent.send_signal_group(meta, 15)

    assert calls == []


@pytest.mark.parametrize("field,value", [("id", 123), ("id", True), ("invocation_id", 123), ("invocation_id", True), ("invocation_id", "")])
def test_group_signal_rejects_malformed_marker_authority(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    """Present malformed marker identities are never stringified into authority."""
    meta = dict(BASE_META)
    meta[field] = value
    calls: list[str] = []
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: calls.append("pin") or None)
    monkeypatch.setattr(
        agent,
        "_pinned_invocation_members",
        lambda _pgid, _aid, _iid: calls.append("scan") or [],
    )

    agent.send_signal_group(meta, 15)

    assert calls == []


@pytest.mark.parametrize("value", [4242.9, "4242", True, 0, -1])
def test_group_alive_fails_closed_on_malformed_present_pgid(
    monkeypatch: pytest.MonkeyPatch,
    value: object,
) -> None:
    """Malformed present group identity is ambiguous/alive, never coerced or gone."""
    meta = dict(BASE_META)
    meta["pgid"] = value
    calls: list[object] = []
    monkeypatch.setattr(
        agent,
        "_proven_invocation_members",
        lambda pgid, _aid, _iid: calls.append(pgid) or ([], True),
    )

    assert agent.group_alive(meta) is True
    assert calls == []


def test_group_alive_preserves_absent_group_semantics() -> None:
    """Actual absence of a recorded PGID remains a proven-empty legacy case."""
    meta = dict(BASE_META)
    meta["pgid"] = None

    assert agent.group_alive(meta) is False


@pytest.mark.parametrize("field,value", [("id", 123), ("id", True), ("invocation_id", 123), ("invocation_id", True), ("invocation_id", "")])
def test_group_alive_fails_closed_on_malformed_marker_authority(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    """Malformed exact markers block positive empty-group proof without scanning."""
    meta = dict(BASE_META)
    meta[field] = value
    scans: list[tuple[int, str, str]] = []
    monkeypatch.setattr(agent, "is_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_recorded_leader_state", lambda _meta: "gone")
    monkeypatch.setattr(
        agent,
        "_proven_invocation_members",
        lambda pgid, aid, iid: scans.append((pgid, aid, iid)) or ([], True),
    )

    assert agent.group_alive(meta) is True
    assert scans == []


@pytest.mark.parametrize("field,value", [("id", 123), ("id", True), ("invocation_id", 123), ("invocation_id", True), ("invocation_id", "")])
def test_leader_marker_state_rejects_malformed_present_markers(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    """Malformed marker metadata yields ambiguity without probing a fabricated marker."""
    meta = dict(BASE_META)
    meta[field] = value
    probes: list[Any] = []
    monkeypatch.setattr(
        agent,
        "env_has_marker",
        lambda pid, aid: probes.append((pid, aid)) or True,
    )

    assert agent._leader_marker_state(meta, 4242) is None
    assert probes == []


def test_valid_group_authority_still_reaches_exact_member_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Canonical integer/string authority retains exact pinned-member convergence."""
    calls: list[tuple[int, str, str]] = []
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: None)
    monkeypatch.setattr(
        agent,
        "_pinned_invocation_members",
        lambda pgid, aid, iid: calls.append((pgid, aid, iid)) or [],
    )

    agent.send_signal_group(dict(BASE_META), 15)

    assert calls == [(4242, "ab12", "inv1")]
