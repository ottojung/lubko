"""Regressions for missing managed-agent marker identity authority."""

import pytest

from lubko import agent

INVOCATION_ID = "0123456789abcdef0123456789abcdef"
BASE_META: agent.Meta = {
    "id": "ab12",
    "pid": 4242,
    "pgid": 4242,
    "start_time": 1234,
    "invocation_id": INVOCATION_ID,
}


def _meta_without_valid_id(mode: str) -> agent.Meta:
    """Return invocation metadata with absent or explicit-null agent identity."""
    meta = dict(BASE_META)
    if mode == "missing":
        meta.pop("id")
    else:
        meta["id"] = None
    return meta


@pytest.mark.parametrize("mode", ["missing", "none"])
def test_group_signal_rejects_missing_agent_id(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Missing agent identity cannot authorize leader pinning or member scans."""
    calls: list[str] = []
    monkeypatch.setattr(agent, "open_pidfd", lambda _pid: calls.append("pin"))
    monkeypatch.setattr(
        agent,
        "_pinned_invocation_members",
        lambda _pgid, _aid, _iid: calls.append("scan") or [],
    )

    agent.send_signal_group(_meta_without_valid_id(mode), 15)

    assert calls == []


@pytest.mark.parametrize("mode", ["missing", "none"])
def test_group_alive_fails_closed_on_missing_agent_id(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Missing agent identity remains ambiguous and blocks empty-group proof."""
    scans: list[tuple[int, str, str]] = []
    monkeypatch.setattr(agent, "is_alive", lambda _meta: False)
    monkeypatch.setattr(agent, "_recorded_leader_state", lambda _meta: "gone")

    def member_probe(pgid: int, aid: str, iid: str) -> tuple[list[tuple[int, int]], bool]:
        scans.append((pgid, aid, iid))
        return [], True

    monkeypatch.setattr(agent, "_proven_invocation_members", member_probe)

    assert agent.group_alive(_meta_without_valid_id(mode)) is True
    assert scans == []


@pytest.mark.parametrize("mode", ["missing", "none"])
def test_leader_marker_state_rejects_missing_agent_id(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Missing agent identity is never normalized into an environment marker."""
    probes: list[tuple[int, str]] = []

    def env_probe(pid: int, aid: str) -> bool:
        probes.append((pid, aid))
        return True

    monkeypatch.setattr(agent, "env_has_marker", env_probe)

    assert agent._leader_marker_state(_meta_without_valid_id(mode), 4242) is None
    assert probes == []
