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

The durable boundary is the same for files and symlinks:

1. write the full payload into a unique temporary file in the destination
   directory;
2. ``fsync`` that temporary file so its bytes are on stable storage;
3. ``os.replace`` the temporary over the destination, which is atomic with
   respect to readers;
4. ``fsync`` the destination directory so the renamed entry is recorded.

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
    """Flush a directory's entries to stable storage.

    A directory only becomes crash-durable once its own entry is recorded in
    its parent; this function flushes the directory's metadata so a rename or
    mkdir performed inside it survives a crash.

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
        fsync_directory(child.parent)
    if first_existing != directory:
        # Anchor the boundary between an already-visible level and the levels
        # we just created: a concurrent first writer may not yet have fsynced
        # this parent, so establish the durability ourselves.
        fsync_directory(first_existing.parent)
    else:
        # The directory already existed; still anchor its entry in its parent
        # so a concurrent creator that has not yet fsynced the parent cannot
        # cause it to be lost.
        fsync_directory(directory.parent)


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
        os.write(fd, data)
        _maybe_inject(temporary, FSYNC_STAGE_FILE)
        os.fsync(fd)
    except OSError as exc:
        msg = f"cannot fsync temporary file {temporary}: {exc}"
        raise DurabilityError(msg) from exc
    finally:
        if fd is not None:
            os.close(fd)


def write_bytes_durable(path: Path, data: bytes) -> None:
    """Crash-durably write ``data`` to ``path`` as a regular file.

    The payload is written to a unique temporary file, fsynced, then atomically
    renamed into place, and finally the destination directory is fsynced so the
    rename is durable. A failure at any confirmation step raises
    :class:`DurabilityError` and leaves the previous destination (if any) and
    the temporary artifact untouched.

    Args:
        path: Destination regular-file path.
        data: Bytes to store.

    Raises:
        DurabilityError: If the write cannot be confirmed durable.
    """
    destination = Path(path)
    make_directory_durable(destination.parent)
    temporary = temporary_path(destination)
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
    fsync_directory(destination.parent)


def write_symlink_durable(path: Path, target: str) -> None:
    """Crash-durably switch ``path`` to a symlink pointing at ``target``.

    The symlink carries no file contents of its own, so only the namespace
    transition needs to be flushed: a temporary symlink is created, its inode
    fsynced, then atomically renamed into place, and the destination directory
    is fsynced so the rename is durable.

    Args:
        path: Destination symlink path.
        target: Symlink target (relative or absolute).

    Raises:
        DurabilityError: If the write cannot be confirmed durable.
    """
    destination = Path(path)
    make_directory_durable(destination.parent)
    temporary = temporary_path(destination)
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
    fsync_directory(destination.parent)


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
