"""Control-socket responsiveness while supervisor readiness work is in flight."""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pytest

from lubko import lifecycle, supervise, supervisor
from lubko.control_socket import (
    _recv_message,
    _send_message,
    bind_abstract_socket,
    connect_abstract_socket,
)


def test_readiness_wait_services_control_requests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A long readiness probe must not starve the supervisor control socket."""
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    listener = bind_abstract_socket()
    daemon._control_sock = listener
    client_go = threading.Event()
    responses: list[dict[str, object] | None] = []
    failures: list[Exception] = []

    def client() -> None:
        client_go.wait()
        conn = None
        try:
            conn = connect_abstract_socket(timeout=0.25)
            conn.settimeout(0.25)
            _send_message(conn, {"type": "ping"})
            responses.append(_recv_message(conn))
        except (OSError, TypeError, ValueError) as exc:  # pragma: no cover - asserted below
            failures.append(exc)
        finally:
            if conn is not None:
                conn.close()

    thread = threading.Thread(target=client)
    thread.start()

    def slow_probe(
        _worker_id: str,
        _cwd: str,
        _pid: int,
        _timeout: float,
        progress_callback: Callable[[], None] | None = None,
    ) -> bool:
        assert progress_callback is not None
        client_go.set()
        deadline = time.monotonic() + 0.6
        while time.monotonic() < deadline:
            progress_callback()
            time.sleep(0.02)
        return False

    monkeypatch.setattr(lifecycle, "verify_worker_consumes_queue", slow_probe)
    child = supervise.WorkerChild(
        pid=os.getpid(),
        pgid=os.getpid(),
        sid=os.getpid(),
        start_time_ticks=1,
        token="a" * 32,
        worker_id="test-worker",
        spawned_at=time.time(),
    )
    try:
        ready, reason = daemon._check_readiness(child, str(tmp_path))
    finally:
        listener.close()
        daemon._control_sock = None
    thread.join(timeout=1.0)

    assert ready is False
    assert reason == "queue consumption not proven"
    assert failures == []
    assert responses == [{"ok": True}]
