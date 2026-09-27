"""Control-socket responsiveness while supervisor readiness work is in flight."""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import socket
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

_STARVATION_TIMEOUT = 2.0
"""Failure-path only: how long a pumped client may go unserved before failing.

Never elapsed when the control socket is healthy, where the response is
already in the socket buffer by the time the probe pumps it.
"""


def _assert_authority_snapshot(response: dict[str, object] | None) -> None:
    """Require one successful authority-snapshot response."""
    assert response is not None
    assert response["ok"] is True
    assert response["desired"] is None
    assert isinstance(response["state"], dict)


def _blocking_probe(
    client_go: threading.Event,
    client_sent: threading.Event,
    client_served: threading.Event,
) -> Callable[..., bool]:
    """Build a ``verify_worker_consumes_queue`` stand-in that blocks on its client.

    ``_check_readiness`` passes the supervisor's control-request pump to the
    probe as ``progress_callback``; this stand-in forces that pump to be the
    thing that serves the client, rather than merely giving it time to be
    served.  It cannot return until the client holding an unanswered request
    has been served, so the probe is still "in flight" at the moment the
    control socket is exercised.

    The ordering is a three-step handshake, each step gated on the previous
    one having actually happened, so no wall-clock window is needed:

    1. the probe releases the client (``client_go``);
    2. the client connects and writes its request (``client_sent``);
    3. the probe pumps the control socket and then waits for the client's
       response (``client_served``).

    Step 3 is the invariant.  ``client_served`` is set only once the client
    holds a complete, successful response, and the probe's own wait is
    strictly longer than the client's socket timeout, so a starved control
    socket can only surface as this probe's assertion - never as a
    timeout error raised by the client that the test would then have to
    diagnose from the outside.  That is deliberate: the test must fail *on the
    starvation*, not on some weaker downstream symptom of it.

    ``client_go`` is set before the ``progress_callback`` assertion so that a
    missing pump still releases a waiting client rather than stranding it.

    Returns:
        A callable matching ``lifecycle.verify_worker_consumes_queue`` that
        always reports "not proven".
    """

    def probe(
        _worker_id: str,
        _cwd: str,
        _pid: int,
        _timeout: float,
        progress_callback: Callable[[], None] | None = None,
    ) -> bool:
        client_go.set()
        assert progress_callback is not None, "probe was given no control-request pump"
        assert client_sent.wait(timeout=_STARVATION_TIMEOUT), (
            "client never sent its request; the probe did not release it"
        )
        progress_callback()
        assert client_served.wait(timeout=_STARVATION_TIMEOUT), (
            "the readiness probe starved the supervisor control socket: a client "
            "connecting while the probe was in flight was not served"
        )
        return False

    return probe


def test_readiness_wait_services_control_requests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A long readiness probe must not starve the supervisor control socket.

    The overlap between "probe in flight" and "client waiting" is established
    by a handshake rather than by a wall-clock window: the probe cannot return
    until the client holding an unanswered request has been served, and the
    client cannot ask until the probe has signalled that it is running.  So the
    ordering under test is forced, not raced, and the healthy path needs no
    sleep and no deadline.

    This asserts the production invariant directly.  A client that connects
    while a readiness probe is in flight must be answered by the
    control-request pump that ``_check_readiness`` hands the probe, and the
    probe's own ``client_served`` assertion is what fails if it is not.
    """
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    monkeypatch.setenv("LUBKO_SUPERVISOR_STATE_TOKEN", "f" * 64)
    supervise.supervisor_private_dir().mkdir(parents=True, exist_ok=True)
    supervise.write_state(supervise.fresh_state())
    daemon = supervisor.SupervisorDaemon(supervisor.Settings())
    listener = bind_abstract_socket()
    daemon._control_sock = listener
    client_go = threading.Event()
    client_sent = threading.Event()
    client_served = threading.Event()
    responses: list[dict[str, object] | None] = []
    failures: list[Exception] = []

    def request() -> socket.socket:
        conn = connect_abstract_socket(timeout=0.25)
        conn.settimeout(0.25)
        _send_message(conn, {"type": "authority_snapshot"})
        return conn

    def client() -> None:
        # The client is a daemon and every wait it does is bounded, so a
        # broken supervisor cannot wedge the interpreter on this thread.
        if not client_go.wait(timeout=_STARVATION_TIMEOUT):
            failures.append(TimeoutError("probe never released the client"))
            return
        conn = None
        try:
            conn = request()
            client_sent.set()
            responses.append(_recv_message(conn))
            client_served.set()
        except (OSError, TypeError, ValueError) as exc:
            # Recorded, not raised: the probe's starvation assertion is the
            # diagnostic that should surface, and it fires first.
            failures.append(exc)
        finally:
            if conn is not None:
                conn.close()

    thread = threading.Thread(target=client, daemon=True)
    thread.start()

    monkeypatch.setattr(
        lifecycle,
        "verify_worker_consumes_queue",
        _blocking_probe(client_go, client_sent, client_served),
    )
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
    thread.join(timeout=_STARVATION_TIMEOUT)
    assert not thread.is_alive(), "control-socket client never finished"

    assert ready is False
    assert reason == "queue consumption not proven"
    assert failures == []
    assert len(responses) == 1
    _assert_authority_snapshot(responses[0])
