"""Additional fail-closed regressions for legacy invocation group authority."""

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


@pytest.mark.parametrize("value", [4242.9, "4242", True, False, 0, -1, ""])
def test_group_alive_rejects_malformed_legacy_pgid_without_group_probe(
    monkeypatch: pytest.MonkeyPatch,
    value: object,
) -> None:
    """Malformed present legacy PGID is ambiguity, not absence or a coerced group."""
    meta = dict(BASE_META)
    meta["invocation_id"] = None
    meta["pgid"] = value
    probes: list[int] = []

    def group_probe(pgid: int) -> bool:
        probes.append(pgid)
        return False

    monkeypatch.setattr(agent, "group_has_members", group_probe)

    assert agent.group_alive(meta) is True
    assert probes == []


def test_group_alive_preserves_canonical_legacy_pgid_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A canonical positive integer legacy PGID still uses the legacy group probe."""
    meta = dict(BASE_META)
    meta["invocation_id"] = None
    probes: list[int] = []

    def group_probe(pgid: int) -> bool:
        probes.append(pgid)
        return False

    monkeypatch.setattr(agent, "group_has_members", group_probe)

    assert agent.group_alive(meta) is False
    assert probes == [4242]


@pytest.mark.parametrize("value", [4242.9, "4242", True, False, 0, -1, ""])
def test_group_alive_rejects_malformed_pid_before_any_group_or_member_probe(
    monkeypatch: pytest.MonkeyPatch,
    value: object,
) -> None:
    """Malformed persisted leader PID blocks convergence before group/member probing."""
    meta = dict(BASE_META)
    meta["pid"] = value
    calls: list[tuple[str, object]] = []

    def group_probe(pgid: int) -> bool:
        calls.append(("group", pgid))
        return False

    def member_probe(pgid: int, _aid: str, _iid: str) -> tuple[list[tuple[int, int]], bool]:
        calls.append(("members", pgid))
        return [], True

    monkeypatch.setattr(agent, "group_has_members", group_probe)
    monkeypatch.setattr(agent, "_proven_invocation_members", member_probe)

    assert agent.group_alive(meta) is True
    assert calls == []
