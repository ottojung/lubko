"""Gate wrapper spawned by the worker before executing a job's user argv.

The worker starts this tiny process (instead of the user program directly) as
the dedicated session/process-group leader. It blocks reading a single control
byte from an inherited gate file descriptor until the worker has durably
persisted the exact process identity (PID/PGID/start-time ticks) to PostgreSQL.
Only then does it release the gate and ``exec`` the user argv on the exact same
PID.

If the worker dies before releasing the gate, the kernel closes the gate's
write end, this process reads EOF, and it exits WITHOUT executing the user
program — so a forced SIGKILL in the spawn->persist window can never leave a
user side effect running with no durable identity to recover it. The gate is
never implemented with ``preexec_fn``: that would let ``Popen`` return only
after the child had already run, defeating the gate.

The gate file-descriptor number is passed in the ``LUBKO_START_GATE_FD``
environment variable rather than as an ``argv`` element. The Python interpreter
may itself be a launcher (for example a virtual-environment ``python`` shim)
that re-executes the real interpreter and shifts ``argv`` so the script path
lands at ``argv[0]``; an environment variable is immune to that shift, whereas
a fixed ``argv`` offset would not be.
"""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from typing import Final

#: Control byte the worker writes to release the gate (allowing the user argv
#: to be exec'd). Any other outcome (EOF, a different byte, or an unreadable
#: gate) means the gate was not released and the user program must not run.
GATE_RELEASE_BYTE: bytes = b"\x01"

#: Environment variable carrying the inherited gate file-descriptor number.
START_GATE_FD_ENV: Final = "LUBKO_START_GATE_FD"


def _main() -> None:
    """Block on the gate, then exec the user argv or exit without side effect."""
    raw_fd = os.environ.get(START_GATE_FD_ENV)
    if not raw_fd:
        os._exit(0)
    try:
        gate_fd = int(raw_fd)
    except ValueError:
        os._exit(0)
    user_argv = sys.argv[1:]
    if not user_argv:
        os._exit(0)
    try:
        control = os.read(gate_fd, 1)
    except OSError:
        control = b""
    # The gate descriptor and its marker must not leak into the user program.
    with suppress(OSError):
        os.environ.pop(START_GATE_FD_ENV, None)
    if control == GATE_RELEASE_BYTE:
        # Release: exec the user argv on this exact PID. The gate read end is
        # closed first so the user program never inherits it.
        with suppress(OSError):
            os.close(gate_fd)
        try:
            os.execvp(user_argv[0], user_argv)
        except OSError:
            os._exit(127)
    # No release: the worker never recorded our identity (or died first). Exit
    # without executing any user code so no unowned side effect survives.
    with suppress(OSError):
        os.close(gate_fd)
    os._exit(0)


if __name__ == "__main__":
    _main()
