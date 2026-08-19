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

The module exposes fault-injection hooks (:func:`set_fsync_failure_injector`)
used only by the test suite to deterministically simulate ``fsync``/replace
failures at the storage-confirmation boundary.
"""

from __future__ import annotations

import json
import os
import secrets
from collections.abc import Callable
from pathlib import Path
from typing import Final

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
    on crash and must not drive a lifecycle action.  This implementation
    therefore prefers to restore the prior destination durably (best effort);
    only if that restore also cannot be confirmed does it fail closed, so
    readers and actions can never treat the transition as committed.

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
        if _restore and previous is not None:
            try:
                write_bytes_durable(destination, previous, _restore=False)
            except DurabilityError:
                pass
            else:
                return
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
    the write is *not* confirmed; the prior pointer is restored durably when
    possible, and only if that also cannot be confirmed is the error raised, so
    the transition is never treated as committed.

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
    temporary = temporary_path(destination)
    previous = str(destination.readlink()) if destination.is_symlink() else None
    try:
        temporary.unlink(missing_ok=True)
        temporary.symlink_to(target)
        # The symlink carries no file contents, so the only durable boundary
        # before the namespace switch is the rename itself; signal the file
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
        if _restore and previous is not None:
            try:
                write_symlink_durable(destination, previous, _restore=False)
            except DurabilityError:
                pass
            else:
                return
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
    try:
        destination.unlink(missing_ok=True)
    except OSError as exc:
        msg = f"cannot remove durable state {destination}: {exc}"
        raise DurabilityError(msg) from exc
    # Confirm the removal is durable: the parent directory no longer references
    # the entry.  A failure here means the removal is not confirmed, so callers
    # must not treat the state as gone.
    fsync_directory(destination.parent)
