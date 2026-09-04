"""Strict runner-reservation generation authority regressions."""

from __future__ import annotations

import copy
import subprocess
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from lubko import agent


@pytest.mark.parametrize("generation", [True, 1.0, "1", [], {}, 0, -1, None])
def test_runner_refuses_malformed_reserved_generation_before_claim(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    generation: object,
) -> None:
    """Malformed durable generation cannot claim runner execution authority."""
    meta = agent.idle_meta("audit", str(tmp_path), None)
    meta.update(
        active_runner=True,
        pending_prompt="work",
        runner_reservation={
            "state": "reserved",
            "gen": generation,
            "mode": "new",
        },
    )
    before = copy.deepcopy(meta)
    monkeypatch.setenv("LUBKO_RUNNER_GEN", "1")
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)

    def fake_update_meta(_aid: str, mutate: object) -> agent.Meta:
        mutate(meta)  # type: ignore[operator]
        return meta

    monkeypatch.setattr(agent, "update_meta", fake_update_meta)
    monkeypatch.setattr(
        agent,
        "_runner_loop",
        lambda *_a, **_kw: pytest.fail("malformed generation reached execution"),
    )

    agent.runner("audit", "new")

    assert meta == before


@pytest.mark.parametrize("generation", [True, 1.0, "1", 0, -1])
def test_spawn_runner_rejects_noncanonical_explicit_generation(
    monkeypatch: pytest.MonkeyPatch,
    generation: object,
) -> None:
    """The explicit spawn boundary never int-coerces generation authority."""
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_a, **_kw: pytest.fail("invalid generation reached subprocess"),
    )

    with pytest.raises(ValueError, match="runner generation is malformed"):
        agent.spawn_runner("audit", "new", gen=generation)  # type: ignore[arg-type]


@pytest.mark.parametrize("generation", [True, 1.0, "1", [], {}, 0, -1, None])
def test_spawn_runner_rejects_malformed_persisted_generation(
    monkeypatch: pytest.MonkeyPatch,
    generation: object,
) -> None:
    """Metadata fallback never truthiness/int-coerces persisted generation."""
    meta: agent.Meta = {
        "runner_reservation": {
            "state": "reserved",
            "gen": generation,
            "mode": "new",
        }
    }
    monkeypatch.setattr(agent, "read_meta", lambda _aid: meta)
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *_a, **_kw: pytest.fail("invalid generation reached subprocess"),
    )

    with pytest.raises(ValueError, match="runner generation is malformed"):
        agent.spawn_runner("audit", "new")


def test_spawn_runner_preserves_canonical_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A canonical positive integer is carried unchanged into the runner env."""
    captured: dict[str, object] = {}

    def fake_popen(*_args: object, **kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    agent.spawn_runner("audit", "new", gen=7)

    env = captured["env"]
    assert isinstance(env, dict)
    assert env["LUBKO_RUNNER_GEN"] == "7"
