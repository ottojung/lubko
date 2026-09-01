# ruff: file-ignore[undocumented-public-module, undocumented-public-function]
import copy

import pytest

from lubko import agent


def test_queue_steer_preserves_fifo_and_sequence() -> None:
    meta: agent.Meta = {"steer_queue": [], "steer_seq": 0}
    agent._queue_steer(meta, "one", 1.0)
    agent._queue_steer(meta, "two", 2.0)
    assert meta["steer_seq"] == 2
    assert meta["steer_queue"] == [
        {"seq": 1, "prompt": "one", "queued_at": 1.0},
        {"seq": 2, "prompt": "two", "queued_at": 2.0},
    ]


@pytest.mark.parametrize("bad", [False, "1", 1.5, -1, {}, []])
def test_queue_steer_rejects_malformed_sequence_without_mutation(bad: object) -> None:
    meta: agent.Meta = {"steer_queue": [], "steer_seq": bad}
    before = copy.deepcopy(meta)
    with pytest.raises(agent.MalformedSteerMetadataError):
        agent._queue_steer(meta, "new", 3.0)
    assert meta == before


@pytest.mark.parametrize("bad", [False, 0, "", {}, 1])
def test_queue_steer_rejects_nonlist_queue_without_mutation(bad: object) -> None:
    meta: agent.Meta = {"steer_queue": bad, "steer_seq": 2}
    before = copy.deepcopy(meta)
    with pytest.raises(agent.MalformedSteerMetadataError):
        agent._queue_steer(meta, "new", 3.0)
    assert meta == before


@pytest.mark.parametrize(
    "item",
    [
        {},
        {"seq": False, "prompt": "x", "queued_at": 1.0},
        {"seq": "1", "prompt": "x", "queued_at": 1.0},
        {"seq": 1.5, "prompt": "x", "queued_at": 1.0},
        {"seq": -1, "prompt": "x", "queued_at": 1.0},
        {"seq": 1, "prompt": 7, "queued_at": 1.0},
        {"seq": 1, "prompt": "x", "queued_at": False},
        {"seq": 1, "prompt": "x", "queued_at": float("nan")},
    ],
)
def test_pop_rejects_malformed_queue_items_without_consuming(item: object) -> None:
    meta: agent.Meta = {"steer_queue": [item], "steer_seq": 2, "prompt_count": 0}
    before = copy.deepcopy(meta)
    assert agent._pop_into_pending(meta, 4.0) is None
    assert meta == before


def test_pop_consumes_canonical_head() -> None:
    meta: agent.Meta = {
        "steer_queue": [
            {"seq": 1, "prompt": "one", "queued_at": 1.0},
            {"seq": 2, "prompt": "two", "queued_at": 2.0},
        ],
        "steer_seq": 2,
        "prompt_count": 0,
    }
    item = agent._pop_into_pending(meta, 4.0)
    assert item == {"seq": 1, "prompt": "one", "queued_at": 1.0}
    assert meta["pending_prompt"] == "one"
    assert meta["steer_queue"] == [{"seq": 2, "prompt": "two", "queued_at": 2.0}]


@pytest.mark.parametrize("bad", [False, "1", 1.5, -1, {}, []])
def test_pop_rejects_malformed_sequence_without_consuming(bad: object) -> None:
    meta: agent.Meta = {
        "steer_queue": [{"seq": 1, "prompt": "one", "queued_at": 1.0}],
        "steer_seq": bad,
        "prompt_count": 0,
    }
    before = copy.deepcopy(meta)
    assert agent._pop_into_pending(meta, 4.0) is None
    assert meta == before


@pytest.mark.parametrize("bad", [False, 0, "", {}, 1])
def test_pop_rejects_nonlist_queue_without_consuming(bad: object) -> None:
    meta: agent.Meta = {"steer_queue": bad, "steer_seq": 2, "prompt_count": 0}
    before = copy.deepcopy(meta)
    assert agent._pop_into_pending(meta, 4.0) is None
    assert meta == before
