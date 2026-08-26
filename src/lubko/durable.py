"""Crash-durable writes for Lubko authoritative recovery/control state.

Lubko keeps several small pieces of state that are the *authority* used to
recover after a crash or a restart:

* ``worker/meta.json`` — worker lifecycle metadata;
* ``worker/rollback.json`` — the supervised-deployment rollback mission;
* ``supervisor/desired.json`` — the explicit supervisor run intent;
* ``supervisor/state.json`` — the daemon's own applied generation and mode;
* ``supervisor/supervisor.pid`` — the exact live supervisor identity;
* ``supervisor/supervisor-runtime`` — the override authorizing which
  supervisor runtime starts;
* ``cli/current`` — the maintained CLI active-pointer symlink;
* ``toolchain.json`` — the recorded ``uv`` executable.

These must survive a power loss exactly: a crash after a write returns must
never lose the just-written value and must never expose a torn value to a
concurrent reader. Observation-only status and health (``supervisor/status.json``,
``worker/health.json`` and its symlinks) are *not* recovery authority and may
stay lightweight atomic writes; they are documented as such at their call sites
and are intentionally not routed through this module.

The durable boundary for regular files is:

1. write the full payload (retrying short ``os.write`` results until complete)
   into a unique temporary file in the destination directory;
2. ``fsync`` that temporary file so its bytes are on stable storage;
3. ``os.replace`` the temporary over the destination, which is atomic with
   respect to readers;
4. ``fsync`` the destination directory so the renamed entry is recorded.

A symlink carries no file contents, so the Linux boundary is the same rename
plus the containing-directory ``fsync``: there is no portable way (and no need)
to ``fsync`` the symlink inode itself, because the rename and the parent
directory ``fsync`` record the new directory entry and flush the inode that
holds the target string.

If any confirmation step cannot be completed the write raises
:class:`DurabilityError` and the caller must treat the value as *not* written:
it must not advance any irreversible lifecycle action that depended on the
durable value being confirmed. The previous destination, if any, is left
untouched and the temporary artifact is removed so a later reader can never
mistake an in-progress or partial write for committed state.

A missing destination directory is created top-down and every newly created
level's entry is recorded in its parent by an immediate parent ``fsync``.  Only
the direct parent of the boundary level is anchored for an already-visible
hierarchy (a concurrent first writer may not yet have fsynced it); this keeps
overhead bounded at one extra directory ``fsync`` per write rather than walking
every ancestor up to the filesystem root.  Because every writer fsyncs its own
direct parent, the whole chain is durable by induction: the root is always
durable, and each level ``d`` is durable once ``d``'s parent has been fsynced.

Temporary names are unique per write (PID + random suffix), so concurrent
writers can never truncate or rewrite one another's in-progress temporary file
even when a destination is reachable by more than one process.

All three primitives serialize same-path durable operations: a stable per-
destination sidecar ``flock`` (plus an in-process reentrant gate) is held from
before the prior state is snapshotted until confirmation or fail-closed
cleanup has fully completed.  A failing unconfirmed transition therefore
always finishes its restore/neutralize before any other writer can proceed,
so cleanup can never revert a newer independently committed value — including
byte-identical (ABA) values, because the interleaving cannot occur.

The module exposes fault-injection hooks (:func:`set_fsync_failure_injector`)
used only by the test suite to deterministically simulate ``fsync``/replace
failures at the storage-confirmation boundary.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import secrets
import threading
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterator

# ``os.write`` may return fewer bytes than requested, so all durable writes
# loop until the whole payload is written; tests can monkeypatch this alias
# to simulate a short write deterministically.
_os_write = os.write

DURABLE_TEMP_PREFIX: Final = ".lubko-durable-"

# Injection stages exercised by the regression suite.
FSYNC_STAGE_FILE: Final = "file"  # the temporary file/symlink itself
FSYNC_STAGE_REPLACE: Final = "replace"  # the atomic rename boundary
FSYNC_STAGE_DIR: Final = "dir"  # the destination directory entry

_FsyncInjector = Callable[[Path, str], None] | None
_fsync_failure_injector: list[_FsyncInjector] = [None]


class DurabilityError(RuntimeError):
    """Raised when authoritative state could not be made crash-durable.

    Raising this error means the write was *not* confirmed: the caller must not
    advance any irreversible action that depended on the value being durably
    stored.
    """


def set_fsync_failure_injector(injector: _FsyncInjector) -> None:
    """Install a fault injector called before each ``fsync``/replace step.

    The injector receives ``(path, stage)`` and may raise to simulate a
    storage-confirmation failure at that boundary.  ``stage`` is one of
    :data:`FSYNC_STAGE_FILE`, :data:`FSYNC_STAGE_REPLACE`, or
    :data:`FSYNC_STAGE_DIR`.  Pass ``None`` to disable injection.

    Args:
        injector: Callable invoked at each confirmation stage, or ``None``.
    """
    _fsync_failure_injector[0] = injector


def clear_fsync_failure_injector() -> None:
    """Remove any installed fault injector."""
    _fsync_failure_injector[0] = None


def set_one_shot_fsync_failure_injector(
    *,
    stage: str = FSYNC_STAGE_DIR,
    path: Path | None = None,
) -> None:
    """Fail the next matching confirmation boundary exactly once, then disable.

    Unlike :func:`set_fsync_failure_injector`, the installed injector clears
    itself after firing so a subsequent confirmation attempt — for example the
    best-effort restoration fsync after a failed first write — succeeds.  This
    lets a regression prove that a failed first confirmation still raises even
    though the restoration then succeeds.

    Args:
        stage: The ``FSYNC_STAGE_*`` boundary at which to fail once.
        path: If given, only fire for this exact destination path.
    """

    def _inject(current_path: Path, current_stage: str) -> None:
        if current_stage != stage:
            return
        if path is not None and Path(current_path) != Path(path):
            return
        set_fsync_failure_injector(None)
        msg = f"injected one-shot {current_stage} failure at {current_path}"
        raise DurabilityError(msg)

    set_fsync_failure_injector(_inject)


def _maybe_inject(path: Path, stage: str) -> None:
    """Invoke the installed fault injector, if any.

    Args:
        path: Path under confirmation.
        stage: One of the ``FSYNC_STAGE_*`` constants.
    """
    injector = _fsync_failure_injector[0]
    if injector is not None:
        injector(path, stage)


def fsync_directory(directory: Path) -> None:
    """Flush a directory's entries to stable storage (injection point).

    A directory only becomes crash-durable once its own entry is recorded in
    its parent; this function flushes the directory's metadata so a rename or
    mkdir performed inside it survives a crash.  This is the *final* confirmation
    fsync after a durable write or removal, so the fault injector is invoked
    here; directory-creation anchoring uses :func:`_fsync_directory` instead so
    injected failures deterministically land on this boundary.

    Args:
        directory: Directory to fsync.

    Raises:
        DurabilityError: If the directory cannot be opened or fsynced.
    """
    directory = Path(directory)
    fd = None
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        _maybe_inject(directory, FSYNC_STAGE_DIR)
        os.fsync(fd)
    except OSError as exc:
        msg = f"cannot fsync directory {directory}: {exc}"
        raise DurabilityError(msg) from exc
    finally:
        if fd is not None:
            os.close(fd)


def _fsync_directory(directory: Path) -> None:
    """Flush a directory's entries to stable storage without fault injection.

    Used for directory-creation anchoring, where the injectable confirmation
    boundary is the final post-rename directory fsync in the write/remove
    primitives, not the intermediate creation fsync.

    Args:
        directory: Directory to fsync.

    Raises:
        DurabilityError: If the directory cannot be opened or fsynced.
    """
    directory = Path(directory)
    fd = None
    try:
        fd = os.open(str(directory), os.O_RDONLY)
        os.fsync(fd)
    except OSError as exc:
        msg = f"cannot fsync directory {directory}: {exc}"
        raise DurabilityError(msg) from exc
    finally:
        if fd is not None:
            os.close(fd)


def make_directory_durable(directory: Path) -> None:
    """Ensure ``directory`` exists and its creation is crash-durable.

    A directory only becomes durable once its entry is recorded in its parent
    by fsyncing that parent. A missing nested hierarchy is therefore created
    top-down and every newly created level is recorded by fsyncing its parent
    immediately after ``mkdir``. Because another concurrent first writer may
    have just created the boundary level without yet fsyncing its parent, the
    visible hierarchy's boundary is anchored by a single extra fsync of the
    boundary level's parent; the deeper ancestors are assumed durable from the
    writes that created them. This keeps overhead bounded at one extra directory
    fsync per write instead of walking every ancestor up to the filesystem
    root: by induction the whole chain is durable because every writer fsyncs
    its own direct parent.

    Args:
        directory: Directory that must exist durably.

    Raises:
        DurabilityError: If any level cannot be created or an ancestor cannot
            be fsynced.
    """
    directory = Path(directory)
    missing: list[Path] = []
    current = directory
    while not current.is_dir():
        missing.append(current)
        parent = current.parent
        if parent == current:
            break
        current = parent
    first_existing = current
    for child in reversed(missing):
        try:
            child.mkdir(exist_ok=True)
        except OSError as exc:
            msg = f"cannot create directory {child} durably: {exc}"
            raise DurabilityError(msg) from exc
        _fsync_directory(child.parent)
    if first_existing != directory:
        # Anchor the boundary between an already-visible level and the levels
        # we just created: a concurrent first writer may not yet have fsynced
        # this parent, so establish the durability ourselves.
        _fsync_directory(first_existing.parent)
    else:
        # The directory already existed; still anchor its entry in its parent
        # so a concurrent creator that has not yet fsynced the parent cannot
        # cause it to be lost.
        _fsync_directory(directory.parent)


def temporary_path(destination: Path) -> Path:
    """Return a unique temporary path next to ``destination``.

    The name is unique per write (PID + random suffix), so concurrent writers
    can never truncate or rewrite one another's in-progress temporary file.

    Args:
        destination: Final destination the temporary precedes.

    Returns:
        A temporary path in the same directory as ``destination``.
    """
    destination = Path(destination)
    suffix = secrets.token_hex(8)
    return destination.with_name(f"{DURABLE_TEMP_PREFIX}{os.getpid()}-{suffix}-{destination.name}")


# Short-write simulation (test seam): when set to a fraction in (0, 1), the
# first ``os.write`` of a temporary file writes only that fraction of the
# buffer, so the retry loop is exercised deterministically.  ``None`` disables
# it.  Mirrors :func:`set_fsync_failure_injector` as a fault-injection hook.
_short_write_fraction: list[float | None] = [None]


def set_short_write_injector(fraction: float | None) -> None:
    """Install a deterministic short-write simulator for tests.

    Args:
        fraction: Fraction of the first ``os.write`` to perform (exclusive of
            ``0`` and ``1``), or ``None`` to disable simulation.
    """
    _short_write_fraction[0] = fraction


DURABLE_LOCK_PREFIX: Final = ".lubko-durable-lock-"


class _DestinationLock:
    """Per-destination lock state, stable for the process lifetime.

    Attributes:
        gate: Thread serialization.  A reentrant lock so the *same* thread may
            re-enter through nested fail-closed cleanup calls, while any other
            thread blocks here until the whole snapshot → rename → confirm or
            cleanup sequence has finished.
        fd: The sidecar ``flock`` descriptor, held only while some thread owns
            the gate (``None`` otherwise).
        depth: Reentrancy depth of the owning thread; ``0`` when unowned.
    """

    def __init__(self) -> None:
        self.gate = threading.RLock()
        self.fd: int | None = None
        self.depth = 0

    def acquire(self) -> bool:
        """Acquire the gate; report whether this is the outermost acquisition.

        ``threading.RLock.acquire()`` reports success, not reentrancy level, so
        the outermost/reentrant distinction is tracked with an explicit depth.

        Returns:
            ``True`` when this call transitioned the gate from unowned to
            owned (so the caller must manage the process-wide ``flock``),
            ``False`` for a same-thread reentrant acquisition.
        """
        self.gate.acquire()
        self.depth += 1
        return self.depth == 1

    def release(self) -> None:
        """Release one level of the gate."""
        self.depth -= 1
        self.gate.release()

    def abandon(self) -> None:
        """Undo a failed outermost acquisition before any flock was obtained."""
        self.release()


# Keys are resolved sidecar lock paths; entries live forever so every thread in
# this process serializes on the same :class:`threading.RLock` instance.
_held_locks: Final[dict[str, _DestinationLock]] = {}
_held_locks_guard = threading.Lock()


@contextlib.contextmanager
def _serialized(destination: Path) -> Iterator[None]:
    """Serialize same-path durable operations across processes and threads.

    Two cooperating layers:

    * a stable per-destination :class:`threading.RLock` ("gate") serializes
      threads within this process — another thread blocks before it can even
      snapshot, so it can never interleave with an in-flight operation;
    * a stable sidecar lock file (``.lubko-durable-lock-<name>`` next to the
      destination) held under an exclusive ``flock`` serializes processes.  The
      ``flock`` is acquired by the outermost acquisition only and released when
      the owning thread fully exits; same-thread nested cleanup calls re-enter
      the gate without reacquiring a conflicting ``flock``.

    Holding the lock across the entire snapshot → rename → confirm/cleanup
    sequence makes it impossible for a newer writer to commit between an
    ownership check and cleanup replace: there is no check-then-act window at
    all.  A failing operation finishes its restore/neutralize before any other
    durable operation on the path proceeds, so a failed unconfirmed transition
    can never revert a newer independently committed writer, and identical-value
    ABA cannot arise because the interleaving itself cannot occur.

    The sidecar file is intentionally never removed: unlinking a lock file
    races with waiters holding descriptors on the old inode and can split
    ownership across two files.  The kernel releases a crashed process's
    ``flock`` automatically, so crash-safety needs no cleanup.  Forking while
    holding a destination lock is not supported: the child inherits the shared
    open file description (and therefore the parent's lock) but not coherent
    Python lock state; callers must complete durable operations before fork.

    Args:
        destination: Destination path whose durable operations are serialized.

    Yields:
        ``None`` while the destination lock is held by the caller.

    Raises:
        DurabilityError: If the lock file cannot be opened or locked.
    """
    key = str(destination.parent / f"{DURABLE_LOCK_PREFIX}{destination.name}")
    with _held_locks_guard:
        entry = _held_locks.get(key)
        if entry is None:
            entry = _DestinationLock()
            _held_locks[key] = entry
    outermost = entry.acquire()
    if outermost:
        try:
            fd = os.open(key, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError as exc:
            entry.abandon()
            msg = f"cannot open durable lock {key}: {exc}"
            raise DurabilityError(msg) from exc
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as exc:
            os.close(fd)
            entry.abandon()
            msg = f"cannot acquire durable lock {key}: {exc}"
            raise DurabilityError(msg) from exc
        entry.fd = fd
    try:
        yield
    finally:
        if outermost:
            held_fd = entry.fd
            entry.fd = None
            # Unlock/close failures must never mask the operation's own result,
            # so they are absorbed; the gate level is always released exactly.
            with contextlib.suppress(OSError):
                if held_fd is not None:
                    try:
                        fcntl.flock(held_fd, fcntl.LOCK_UN)
                    finally:
                        os.close(held_fd)
            entry.release()
        else:
            entry.release()


def _write_all(fd: int, data: bytes) -> None:
    """Write every byte of ``data`` to ``fd``, handling short writes.

    ``os.write`` is permitted to transfer fewer bytes than requested, so this
    loops until the whole payload is on the descriptor.  A simulated short
    first write (see :func:`set_short_write_injector`) is applied when set.

    Args:
        fd: Open file descriptor to write to.
        data: Bytes to store.

    Raises:
        OSError: If a write transfers zero or negative bytes.
    """
    view = memoryview(data)
    total = 0
    first = True
    while total < len(data):
        chunk = view[total:]
        if first and _short_write_fraction[0] is not None:
            limit = max(1, int(len(chunk) * _short_write_fraction[0]))
            chunk = chunk[:limit]
        n = _os_write(fd, chunk)
        if n <= 0:
            msg = f"os.write transferred {n} bytes of {len(data) - total} remaining"
            raise OSError(msg)
        total += n
        first = False


def _write_temporary_file(temporary: Path, data: bytes) -> None:
    """Write ``data`` to ``temporary`` and fsync the file bytes.

    Args:
        temporary: Temporary regular-file path to write.
        data: Bytes to store.

    Raises:
        DurabilityError: If the file cannot be written or fsynced.
    """
    fd = None
    try:
        fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        _write_all(fd, data)
        _maybe_inject(temporary, FSYNC_STAGE_FILE)
        os.fsync(fd)
    except OSError as exc:
        msg = f"cannot fsync temporary file {temporary}: {exc}"
        raise DurabilityError(msg) from exc
    finally:
        if fd is not None:
            os.close(fd)


def write_bytes_durable(path: Path, data: bytes, *, _restore: bool = True) -> None:
    """Crash-durably write ``data`` to ``path`` as a regular file.

    The payload is written to a unique temporary file, fully flushed (short
    ``os.write`` results are retried until complete), fsynced, then atomically
    renamed into place, and finally the destination directory is fsynced so the
    rename is durable. A failure at any confirmation step raises
    :class:`DurabilityError`.

    When the rename succeeds but the final directory fsync cannot be confirmed,
    the write is *not* confirmed: the previously visible authority may be lost
    on crash and must not drive a lifecycle action.  This implementation performs
    a best-effort fail-closed cleanup — restoring the prior destination durably
    when one existed, or durably neutralizing the newly visible unconfirmed
    destination for a first write — but it never converts the failure into
    success: the original :class:`DurabilityError` is always re-raised so callers
    cannot advance an action that depended on the new value being committed.

    Args:
        path: Destination regular-file path.
        data: Bytes to store.
        _restore: Internal; disable the best-effort restore on nested calls to
            avoid unbounded recursion.

    Raises:
        DurabilityError: If the write cannot be confirmed durable.
    """
    destination = Path(path)
    make_directory_durable(destination.parent)
    # Serialize the whole snapshot → rename → confirm/cleanup sequence against
    # every other durable operation on this path (across processes and
    # threads).  Because no other writer can even snapshot — let alone commit —
    # while this lock is held, fail-closed cleanup below can never overwrite a
    # newer independently committed value, and payload comparison cannot be
    # fooled by identical-value ABA: the interleaving is simply impossible.
    with _serialized(destination):
        temporary = temporary_path(destination)
        previous = destination.read_bytes() if destination.exists() else None
        try:
            _write_temporary_file(temporary, data)
            _maybe_inject(temporary, FSYNC_STAGE_REPLACE)
            Path(temporary).replace(destination)
        except BaseException as exc:
            temporary.unlink(missing_ok=True)
            if isinstance(exc, DurabilityError):
                raise
            msg = f"cannot durably write {destination}: {exc}"
            raise DurabilityError(msg) from exc
        try:
            fsync_directory(destination.parent)
        except DurabilityError:
            # A nested restore/neutralize attempt (``_restore=False``) must never
            # itself recurse into further restore/neutralize: if the final directory
            # fsync fails again there is nothing more to attempt, so re-raise the
            # current failure immediately and let the top-level best-effort cleanup
            # absorb it.  Only the top-level ``_restore=True`` attempt performs the
            # cleanup below.
            if not _restore:
                raise
            # The rename landed but its directory entry is not confirmed durable.
            # Best-effort fail-closed cleanup so a later reader never observes a
            # torn transition: restore the prior authority when one existed, or
            # durably neutralize the unconfirmed destination for a first write. This
            # cleanup MUST NOT convert the failure into success — the original error
            # is always re-raised so dependent actions cannot advance.  The
            # destination lock is still held here, so the restore cannot race a
            # concurrent writer; the nested calls re-enter the same lock
            # reentrantly.
            if previous is not None:
                with contextlib.suppress(DurabilityError):
                    write_bytes_durable(destination, previous, _restore=False)
            else:
                with contextlib.suppress(DurabilityError):
                    remove_durable(destination)
            raise


def write_symlink_durable(path: Path, target: str, *, _restore: bool = True) -> None:
    """Crash-durably switch ``path`` to a symlink pointing at ``target``.

    A symlink carries no file contents of its own, so the Linux durability
    boundary is: create a temporary symlink, atomically rename it into place,
    then ``fsync`` the containing directory so the new directory entry is
    recorded.  (There is no portable way to fsync the symlink inode itself, and
    none is needed: the rename plus the parent directory fsync records the new
    dentry, and the target string lives in the inode flushed by that same
    directory fsync.)  A failure at any confirmation step raises
    :class:`DurabilityError`.

    When the rename succeeds but the final directory fsync cannot be confirmed,
    the write is *not* confirmed.  This implementation performs a best-effort
    fail-closed cleanup — restoring the prior entry's exact type and value
    durably when one existed (symlink target or regular-file bytes), or
    durably neutralizing the unconfirmed destination for a first write — but
    it never converts the failure into success: the original
    :class:`DurabilityError` is always re-raised so callers cannot advance an
    action that depended on the new pointer being committed.

    Args:
        path: Destination symlink path.
        target: Symlink target (relative or absolute).
        _restore: Internal; disable the best-effort restore on nested calls to
            avoid unbounded recursion.

    Raises:
        DurabilityError: If the write cannot be confirmed durable.
    """
    destination = Path(path)
    make_directory_durable(destination.parent)
    # Same-path serialization as :func:`write_bytes_durable`: the lock is held
    # from the pointer snapshot through confirmation/failure cleanup, so a
    # failed switch can never restore its stale prior pointer over a newer
    # independently committed one.
    with _serialized(destination):
        temporary = temporary_path(destination)
        # Snapshot the prior entry's exact type and value: a symlink's target,
        # a true regular file's bytes, or absence.  The destination may hold any
        # of these, and a failed confirmation must restore exactly what was
        # there before — never delete prior regular-file contents because they
        # are not a symlink.  The regular-file check is lstat-safe and mirrors
        # :func:`remove_durable`: only a non-symlink regular file's bytes are
        # read; directories, FIFOs, devices, and other unsupported entry types
        # are never read as bytes.
        previous_link: str | None = None
        previous_bytes: bytes | None = None
        if destination.is_symlink():
            previous_link = str(destination.readlink())
        elif destination.exists() and destination.is_file() and not destination.is_symlink():
            previous_bytes = destination.read_bytes()
        try:
            temporary.unlink(missing_ok=True)
            temporary.symlink_to(target)
            # The symlink carries no file contents, so the only durable boundary
            # before the rename is the rename itself; signal the file
            # stage here so fault injection can exercise a pre-rename failure.
            _maybe_inject(temporary, FSYNC_STAGE_FILE)
            _maybe_inject(temporary, FSYNC_STAGE_REPLACE)
            Path(temporary).replace(destination)
        except BaseException as exc:
            temporary.unlink(missing_ok=True)
            if isinstance(exc, DurabilityError):
                raise
            msg = f"cannot durably write symlink {destination}: {exc}"
            raise DurabilityError(msg) from exc
        try:
            fsync_directory(destination.parent)
        except DurabilityError:
            # A nested restore/neutralize attempt (``_restore=False``) must never
            # itself recurse into further restore/neutralize: if the final directory
            # fsync fails again there is nothing more to attempt, so re-raise the
            # current failure immediately and let the top-level best-effort cleanup
            # absorb it.  Only the top-level ``_restore=True`` attempt performs the
            # cleanup below.
            if not _restore:
                raise
            # The rename landed but its directory entry is not confirmed durable.
            # Best-effort fail-closed cleanup: restore the prior pointer when one
            # existed, or durably neutralize the unconfirmed destination for a first
            # write. The failure is never converted into success — the original
            # error is always re-raised.  The destination lock is still held here,
            # so the restore cannot race a concurrent writer; the nested calls
            # re-enter the same lock reentrantly.
            # Restore exactly the prior entry: a symlink's target, a regular
            # file's bytes, or — for a first write — durably neutralize the
            # unconfirmed destination.
            if previous_link is not None:
                with contextlib.suppress(DurabilityError):
                    write_symlink_durable(destination, previous_link, _restore=False)
            elif previous_bytes is not None:
                with contextlib.suppress(DurabilityError):
                    write_bytes_durable(destination, previous_bytes, _restore=False)
            else:
                with contextlib.suppress(DurabilityError):
                    remove_durable(destination)
            raise


def write_text_durable(path: Path, text: str) -> None:
    """Crash-durably write ``text`` to ``path`` as UTF-8.

    Args:
        path: Destination regular-file path.
        text: Text to store.

    Note:
        Fails closed: the underlying :func:`write_bytes_durable` raises
        :class:`DurabilityError` when the write cannot be confirmed durable, so
        callers must not advance a dependent action.
    """
    write_bytes_durable(path, text.encode("utf-8"))


def write_json_durable(path: Path, mapping: dict[str, object]) -> None:
    """Crash-durably write ``mapping`` as sorted-key JSON to ``path``.

    Args:
        path: Destination regular-file path.
        mapping: JSON-serializable object to store.

    Note:
        Fails closed: the underlying :func:`write_text_durable` raises
        :class:`DurabilityError` when the write cannot be confirmed durable, so
        callers must not advance a dependent action.
    """
    write_text_durable(path, json.dumps(mapping, sort_keys=True) + "\n")


def remove_durable(path: Path) -> None:
    """Crash-durably remove an authoritative state file.

    The file is unlinked and then its containing directory is fsynced so the
    removal is recorded durably: a crash after this returns must never bring the
    removed authority back.  This is the authoritative counterpart of the
    durable write primitives and must be used for every recovery/control-state
    removal (for example the supervisor-runtime override, the supervisor
    pidfile on shutdown, and rollback-state repair/migration clearing).
    Observation-only status/readiness cleanup is intentionally *not* routed
    through here.

    Args:
        path: Authoritative state file to remove.

    Raises:
        DurabilityError: If the removal cannot be confirmed durable.
    """
    destination = Path(path)
    # Same-path serialization as the write primitives: the lock is held from
    # the prior-state snapshot through confirmation/failure cleanup, so a
    # failed removal can never resurrect stale authority over a newer
    # independently committed writer.
    with _serialized(destination):
        # Snapshot the prior bytes of an existing regular file so a failed
        # confirmation can best-effort restore the authority instead of silently
        # losing it.  The check is lstat-safe: ``is_file()`` follows symlinks, so a
        # symlink (for example the unconfirmed first-write pointer neutralized by
        # ``write_symlink_durable``) must NOT be treated as a regular file holding
        # its target's bytes — restoring those bytes would turn the pointer path
        # into a regular file.  Snapshot only a true regular file that is not a
        # symlink; symlinks and missing entries have no prior bytes to keep.
        previous = (
            destination.read_bytes()
            if (destination.exists() and destination.is_file() and not destination.is_symlink())
            else None
        )
        try:
            destination.unlink(missing_ok=True)
        except OSError as exc:
            msg = f"cannot remove durable state {destination}: {exc}"
            raise DurabilityError(msg) from exc
        # Confirm the removal is durable: the parent directory no longer references
        # the entry.  A failure here means the removal is not confirmed, so callers
        # must not treat the state as gone.
        try:
            fsync_directory(destination.parent)
        except DurabilityError:
            # The removal is not confirmed durable.  Best-effort restore the prior
            # bytes so the authority is not silently lost, but always re-raise the
            # original removal failure: a caller must never treat the state as gone.
            # The destination lock is still held here, so the restore cannot race a
            # concurrent writer; the nested call re-enters the same lock reentrantly.
            if previous is not None:
                with contextlib.suppress(DurabilityError):
                    write_bytes_durable(destination, previous, _restore=False)
            raise
