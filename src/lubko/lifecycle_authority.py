"""Database-backed lifecycle authority for supervisor ownership.

Crash-durable lifecycle authority lives in the existing ``lubko.jobs``
transport table as the ``lifecycle_authority`` application payload kind: one
current-state row per execution server. The table's PostgreSQL metadata is
untouched (``id uuid primary key``, opaque ``payload text``); the new kind is
a pure application-protocol evolution. Server isolation reuses the exact
application-level server predicates, and the row kind is permanently exempt
from worker garbage collection: every GC mutation predicates on
``type = 'command'`` or ``type = 'output_chunk'``, and the authority row is
never terminal.

Uniqueness is mechanical and needs no new database constraint: the row ``id``
is a deterministic UUIDv5 over a fixed Lubko namespace plus the exact server
string, so the frozen primary key already rejects a second row for the same
server. Concurrent bootstraps converge via ``INSERT ... ON CONFLICT DO
NOTHING``: exactly one insert wins and every contender proceeds on the same
row.

The linearization point of every ownership transition is the commit of a
single row update guarded by a compare-and-swap predicate on the row's
fencing epoch. Local files (``supervisor.pid`` and friends) are read-through
caches, never authority: a cache entry is usable only when it matches a
freshly read row, and on any disagreement the row wins. A torn or stale
cache therefore fails closed by construction, and a host with zero free
blocks operates correctly with an absent or outdated cache.

Every ownership-dependent or destructive child action additionally requires
a fresh row read inside the decision whose fencing epoch matches the acting
incarnation's own epoch. A disconnected or superseded incarnation performs
only authority-independent observation until fresh authority is available.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final
from uuid import UUID, uuid5

import psycopg

if TYPE_CHECKING:
    from lubko.worker import JobsConnection

#: Application payload kind carrying crash-durable lifecycle authority. The
#: kind is permanently exempt from worker GC: collection predicates match
#: only ``command``/``output_chunk`` rows, and this row is never terminal.
AUTHORITY_TYPE: Final = "lifecycle_authority"

#: Supported authority payload version. Any other version fails closed.
#: Version 2 is the single canonical schema: ``spawn`` and ``worker`` keys
#: must be present explicitly (null when absent). Version 1 rows are never
#: parsed by normal runtime code; they are converted once by
#: :func:`migrate_v1_to_v2`.
AUTHORITY_VERSION: Final = 2

#: Previous schema version, accepted only inside :func:`migrate_v1_to_v2`.
LEGACY_AUTHORITY_VERSION: Final = 1

#: Strict maximum size of the serialized authority payload in UTF-8 bytes.
#: Transitions that would exceed the bound are refused before any
#: irreversible action, exactly as deployments are refused today. No history
#: accumulates: the row holds current state only.
AUTHORITY_MAX_BYTES: Final = 4096

#: Fixed namespace binding authority row ids to execution-server names.
#: UUIDv5 over this namespace plus the exact server string derives the row
#: ``id`` deterministically in application code.
AUTHORITY_NAMESPACE: Final = UUID("24b16c63-7210-4b87-9bc6-09583150835d")

#: Bootstrap insert converging concurrent first writers onto one row.
_BOOTSTRAP_INSERT_SQL: Final = (
    "INSERT INTO lubko.jobs (id, payload)\n"
    "VALUES (%(id)s, %(payload)s)\n"
    "ON CONFLICT (id) DO NOTHING"
)

#: Canonical row read by deterministic id.
_READ_ROW_SQL: Final = "SELECT payload FROM lubko.jobs\nWHERE id = %(id)s"

#: Every authority mutation is a true compare-and-swap over the exact
#: observed payload text: the update commits only when the row still holds
#: byte-for-byte the state the decision was based on. An epoch-only guard
#: would let a stale contender clobber a concurrently committed spawn
#: obligation while moving the epoch; the full-state guard makes any such
#: interleaving fail the update so the loser stands down without acting.
_CAS_ROW_SQL: Final = (
    "UPDATE lubko.jobs\n"
    "SET payload = %(payload)s\n"
    "WHERE id = %(id)s\n"
    "    AND payload = %(expected)s"
)


class AuthorityError(RuntimeError):
    """A lifecycle authority row is missing, corrupt, or untrusted.

    Raising this error means no usable authority exists: the caller must
    hold and never act on lifecycle state. It is never raised for mere
    connectivity loss; see :class:`AuthorityUnavailableError`.
    """


class AuthorityUnavailableError(RuntimeError):
    """Lifecycle authority could not be reached right now.

    The database is unreachable or a raced row vanished mid-decision. The
    caller must limit itself to authority-independent observation and retry
    later; a partitioned incarnation is indistinguishable from a superseded
    one, so no ownership-dependent action may follow.
    """


@dataclass(frozen=True, slots=True)
class AuthorityOwner:
    """The exact incarnation holding a fencing epoch."""

    pid: int
    start_time_ticks: int
    boot_id: str


@dataclass(frozen=True, slots=True)
class SpawnObligation:
    """A database-backed fenced pre-spawn obligation.

    This is the crash-durable authority that forbids a successor from
    starting a second maintained consumer beside a possibly-live first
    spawn. It is committed to the authority row *before* the spawn
    syscall; a crash between commit and spawn leaves this deterministic
    recovery obligation in the row. A ``pid`` of ``None`` names a spawn
    whose child identity was never published (resolved via the kernel
    parent-death guarantee and boot identity); a present ``pid`` names
    the exact first-spawn instance to converge by pinned single-PID
    signals only.
    """

    token: str
    commit: str
    creator_pid: int
    creator_start_time_ticks: int
    boot_id: str | None
    pid: int | None
    start_time_ticks: int | None
    parent_death_signal: bool


@dataclass(frozen=True, slots=True)
class WorkerRecord:
    """The database-authoritative published maintained worker.

    This is the crash-durable publication record: once committed, the
    named exact process instance is the maintained consumer, even when
    every local filesystem mutation fails and no local cache names it.
    Publication and pre-spawn-obligation clearance commit atomically, so
    no observer ever sees a published worker beside an outstanding
    obligation for it, and no successor ever spawns beside it.
    """

    token: str
    commit: str
    pid: int
    pgid: int
    sid: int
    start_time_ticks: int
    worker_id: str


@dataclass(frozen=True, slots=True)
class AuthorityRow:
    """A parsed and validated lifecycle authority row."""

    server: str
    epoch: int
    generation: int
    owner: AuthorityOwner | None
    spawn: SpawnObligation | None = None
    worker: WorkerRecord | None = None


@dataclass(frozen=True, slots=True)
class AuthorityClaim:
    """An incarnation's in-memory fencing-epoch holding.

    A claim authorizes nothing by itself: every ownership-dependent action
    must first confirm it against a fresh row read with
    :func:`confirm_authority`.
    """

    server: str
    epoch: int
    pid: int
    start_time_ticks: int
    boot_id: str


def _require_server_identity(value: object) -> str:
    """Validate an execution-server identity for authority derivation.

    Args:
        value: Candidate server value.

    Returns:
        The validated non-empty server string.

    Raises:
        AuthorityError: If the value is not a non-empty string.
    """
    if not isinstance(value, str) or not value:
        msg = "lifecycle authority requires a non-empty server identity"
        raise AuthorityError(msg)
    return value


def authority_row_id(server: str) -> UUID:
    """Derive the deterministic authority row id for an execution server.

    Args:
        server: Exact non-empty execution-server identity.

    Returns:
        The UUIDv5 row id. Distinct servers map to distinct ids; identical
        server names denote the same authority domain by definition.

    Note:
        An empty server identity fails closed via :class:`AuthorityError`
        from :func:`_require_server_identity`.
    """
    validated_server = _require_server_identity(server)
    return uuid5(AUTHORITY_NAMESPACE, validated_server)


def build_authority_payload(
    *,
    server: str,
    epoch: int,
    generation: int,
    owner: AuthorityOwner | None,
) -> dict[str, object]:
    """Build a canonical authority payload mapping.

    The lifecycle sections (``spawn``, ``worker``) default to absent; use
    :func:`canonical_row_text` to serialize a complete row.

    Args:
        server: Exact execution-server identity the row governs.
        epoch: Monotonically increasing fencing epoch.
        generation: Lifecycle generation counter carried by the row.
        owner: Exact incarnation holding the epoch, or ``None`` when the
            epoch is unowned.

    Returns:
        The versioned payload mapping.

    Raises:
        AuthorityError: If any field violates the binding.
    """
    validated_server = _require_server_identity(server)
    epoch = _check_non_negative_int("epoch", epoch)
    generation = _check_non_negative_int("generation", generation)
    owner_mapping: dict[str, object] | None = None
    if owner is not None:
        parsed = _parse_owner({
            "pid": owner.pid,
            "start_time_ticks": owner.start_time_ticks,
            "boot_id": owner.boot_id,
        })
        if parsed is None:
            msg = "authority payload owner must carry an exact process identity"
            raise AuthorityError(msg)
        owner_mapping = {
            "pid": parsed.pid,
            "start_time_ticks": parsed.start_time_ticks,
            "boot_id": parsed.boot_id,
        }
    return {
        "v": AUTHORITY_VERSION,
        "type": AUTHORITY_TYPE,
        "server": validated_server,
        "epoch": epoch,
        "generation": generation,
        "owner": owner_mapping,
        "spawn": None,
        "worker": None,
    }


def canonical_row_text(row: AuthorityRow) -> str:
    """Serialize a complete authority row to canonical guarded text.

    Args:
        row: The parsed row to serialize.

    Returns:
        Canonical sorted-key JSON text within the documented byte bound.

    Note:
        Field violations and bound overruns surface as
        :class:`AuthorityError` from the section builders.
    """
    payload = build_authority_payload(
        server=row.server, epoch=row.epoch, generation=row.generation, owner=row.owner
    )
    payload["spawn"] = _build_spawn_mapping(row.spawn)
    payload["worker"] = _build_worker_mapping(row.worker)
    return serialize_authority_payload(payload)


def _build_spawn_mapping(spawn: SpawnObligation | None) -> dict[str, object] | None:
    """Serialize an optional spawn obligation, validating its binding.

    Args:
        spawn: The obligation to serialize, or ``None``.

    Returns:
        The mapping, or ``None`` when no spawn is outstanding.

    Raises:
        AuthorityError: If any field violates the binding.
    """
    if spawn is None:
        return None
    token: object = spawn.token
    if not isinstance(token, str) or not token:
        msg = "authority spawn obligation must carry a lifecycle token"
        raise AuthorityError(msg)
    commit: object = spawn.commit
    if not isinstance(commit, str) or not commit:
        msg = "authority spawn obligation must carry an exact commit"
        raise AuthorityError(msg)
    creator_pid = _check_non_negative_int("spawn.creator_pid", spawn.creator_pid)
    creator_ticks = _check_non_negative_int(
        "spawn.creator_start_time_ticks", spawn.creator_start_time_ticks
    )
    boot_id: str | None = None
    boot_raw: object = spawn.boot_id
    if boot_raw is not None:
        if not isinstance(boot_raw, str) or not boot_raw:
            msg = "authority spawn obligation must carry a host boot identity or null"
            raise AuthorityError(msg)
        boot_id = boot_raw
    pid: int | None = None
    if spawn.pid is not None:
        pid = _check_non_negative_int("spawn.pid", spawn.pid)
    ticks: int | None = None
    if spawn.start_time_ticks is not None:
        ticks = _check_non_negative_int("spawn.start_time_ticks", spawn.start_time_ticks)
    flag: object = spawn.parent_death_signal
    if not isinstance(flag, bool):
        msg = "authority spawn obligation parent-death-signal flag must be a boolean"
        raise AuthorityError(msg)
    return {
        "token": token,
        "commit": commit,
        "creator_pid": creator_pid,
        "creator_start_time_ticks": creator_ticks,
        "boot_id": boot_id,
        "pid": pid,
        "start_time_ticks": ticks,
        "parent_death_signal": flag,
    }


def _parse_spawn(raw: object) -> SpawnObligation | None:
    """Parse an optional spawn obligation from a payload mapping.

    Args:
        raw: The raw ``spawn`` value.

    Returns:
        The validated obligation, or ``None`` for explicit null.

    Raises:
        AuthorityError: If the value is neither null nor an exact spawn
            obligation. A partial obligation never parses as usable.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = "authority payload spawn must be an object or null"
        raise AuthorityError(msg)
    token = raw.get("token")
    if not isinstance(token, str) or not token:
        msg = "authority spawn obligation must carry a lifecycle token"
        raise AuthorityError(msg)
    commit = raw.get("commit")
    if not isinstance(commit, str) or not commit:
        msg = "authority spawn obligation must carry an exact commit"
        raise AuthorityError(msg)
    creator_pid = _check_non_negative_int("spawn.creator_pid", raw.get("creator_pid"))
    creator_ticks = _check_non_negative_int(
        "spawn.creator_start_time_ticks", raw.get("creator_start_time_ticks")
    )
    boot_raw = raw.get("boot_id")
    boot_id: str | None = None
    if boot_raw is not None:
        if not isinstance(boot_raw, str) or not boot_raw:
            msg = "authority spawn obligation must carry a host boot identity or null"
            raise AuthorityError(msg)
        boot_id = boot_raw
    pid_raw = raw.get("pid")
    pid: int | None = None
    if pid_raw is not None:
        pid = _check_non_negative_int("spawn.pid", pid_raw)
    ticks_raw = raw.get("start_time_ticks")
    ticks: int | None = None
    if ticks_raw is not None:
        ticks = _check_non_negative_int("spawn.start_time_ticks", ticks_raw)
    flag = raw.get("parent_death_signal")
    if not isinstance(flag, bool):
        msg = "authority spawn obligation must carry an explicit boolean parent-death-signal flag"
        raise AuthorityError(msg)
    return SpawnObligation(
        token=token,
        commit=commit,
        creator_pid=creator_pid,
        creator_start_time_ticks=creator_ticks,
        boot_id=boot_id,
        pid=pid,
        start_time_ticks=ticks,
        parent_death_signal=flag,
    )


def _build_worker_mapping(worker: WorkerRecord | None) -> dict[str, object] | None:
    """Serialize an optional published-worker record, validating its binding.

    Args:
        worker: The record to serialize, or ``None``.

    Returns:
        The mapping, or ``None`` when no worker is published.

    Raises:
        AuthorityError: If any field violates the binding.
    """
    if worker is None:
        return None
    token: object = worker.token
    if not isinstance(token, str) or not token:
        msg = "authority worker record must carry a lifecycle token"
        raise AuthorityError(msg)
    commit: object = worker.commit
    if not isinstance(commit, str) or not commit:
        msg = "authority worker record must carry an exact commit"
        raise AuthorityError(msg)
    worker_id: object = worker.worker_id
    if not isinstance(worker_id, str) or not worker_id:
        msg = "authority worker record must carry a worker identity"
        raise AuthorityError(msg)
    return {
        "token": token,
        "commit": commit,
        "pid": _check_positive_int("worker.pid", worker.pid),
        "pgid": _check_non_negative_int("worker.pgid", worker.pgid),
        "sid": _check_non_negative_int("worker.sid", worker.sid),
        "start_time_ticks": _check_non_negative_int(
            "worker.start_time_ticks", worker.start_time_ticks
        ),
        "worker_id": worker_id,
    }


def _parse_worker(raw: object) -> WorkerRecord | None:
    """Parse an optional published-worker record from a payload mapping.

    Args:
        raw: The raw ``worker`` value.

    Returns:
        The validated record, or ``None`` for explicit null.

    Raises:
        AuthorityError: If the value is neither null nor an exact worker
            record. A partial record never parses as a published worker.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = "authority payload worker must be an object or null"
        raise AuthorityError(msg)
    token = raw.get("token")
    if not isinstance(token, str) or not token:
        msg = "authority worker record must carry a lifecycle token"
        raise AuthorityError(msg)
    commit = raw.get("commit")
    if not isinstance(commit, str) or not commit:
        msg = "authority worker record must carry an exact commit"
        raise AuthorityError(msg)
    worker_id = raw.get("worker_id")
    if not isinstance(worker_id, str) or not worker_id:
        msg = "authority worker record must carry a worker identity"
        raise AuthorityError(msg)
    return WorkerRecord(
        token=token,
        commit=commit,
        pid=_check_positive_int("worker.pid", raw.get("pid")),
        pgid=_check_non_negative_int("worker.pgid", raw.get("pgid")),
        sid=_check_non_negative_int("worker.sid", raw.get("sid")),
        start_time_ticks=_check_non_negative_int(
            "worker.start_time_ticks", raw.get("start_time_ticks")
        ),
        worker_id=worker_id,
    )


def serialize_authority_payload(payload: dict[str, object]) -> str:
    """Serialize an authority payload, enforcing the documented byte bound.

    Args:
        payload: Mapping produced by :func:`build_authority_payload`.

    Returns:
        Canonical sorted-key JSON text.

    Raises:
        AuthorityError: If the serialized payload exceeds
            :data:`AUTHORITY_MAX_BYTES`. Oversize transitions are refused
            before any irreversible action.
    """
    text = json.dumps(payload, sort_keys=True)
    if len(text.encode("utf-8")) > AUTHORITY_MAX_BYTES:
        msg = (
            f"authority payload exceeds the {AUTHORITY_MAX_BYTES}-byte bound; "
            "the transition is refused"
        )
        raise AuthorityError(msg)
    return text


def parse_authority_payload(data: object, *, server: str) -> AuthorityRow:
    """Parse and validate a stored authority payload against the binding.

    Args:
        data: The JSON object stored in the ``payload`` column, either as a
            raw JSON string or as an already-decoded mapping.
        server: Exact execution-server identity the row must govern. A row
            naming any other server is untrusted: hold, never act.

    Returns:
        The parsed authority row.

    Raises:
        AuthorityError: If the payload is missing, malformed, of an
            unsupported version or kind, oversize, or governs a different
            server.
    """
    if isinstance(data, str):
        if len(data.encode("utf-8")) > AUTHORITY_MAX_BYTES:
            msg = "authority payload exceeds the documented byte bound"
            raise AuthorityError(msg)
        try:
            data = json.loads(data)
        except json.JSONDecodeError as exc:
            msg = f"authority payload is not valid JSON: {exc}"
            raise AuthorityError(msg) from exc
    if not isinstance(data, dict):
        msg = "authority payload must be a JSON object"
        raise AuthorityError(msg)
    version = data.get("v")
    if version != AUTHORITY_VERSION or isinstance(version, bool):
        msg = f"unsupported lifecycle authority version: {version!r}"
        raise AuthorityError(msg)
    if data.get("type") != AUTHORITY_TYPE:
        msg = f"payload is not lifecycle authority: {data.get('type')!r}"
        raise AuthorityError(msg)
    if data.get("server") != server:
        msg = "lifecycle authority row governs a different server; holding"
        raise AuthorityError(msg)
    if "spawn" not in data or "worker" not in data:
        msg = "authority payload must carry explicit spawn and worker keys"
        raise AuthorityError(msg)
    epoch = _check_non_negative_int("epoch", data.get("epoch"))
    generation = _check_non_negative_int("generation", data.get("generation"))
    owner = _parse_owner(data.get("owner"))
    spawn = _parse_spawn(data.get("spawn"))
    worker = _parse_worker(data.get("worker"))
    if spawn is not None and worker is not None:
        msg = "authority row cannot carry both a spawn obligation and a published worker"
        raise AuthorityError(msg)
    return AuthorityRow(
        server=server, epoch=epoch, generation=generation, owner=owner, spawn=spawn, worker=worker
    )


def _parse_owner(raw: object) -> AuthorityOwner | None:
    """Parse an optional exact owner identity from a payload mapping.

    Args:
        raw: The raw ``owner`` value.

    Returns:
        The validated owner, or ``None`` for explicit null.

    Raises:
        AuthorityError: If the value is neither null nor an exact process
            identity. A partial identity never parses as usable ownership.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        msg = "authority payload owner must be an object or null"
        raise AuthorityError(msg)
    pid = _check_positive_int("owner.pid", raw.get("pid"))
    ticks = _check_non_negative_int("owner.start_time_ticks", raw.get("start_time_ticks"))
    boot_id = raw.get("boot_id")
    if not isinstance(boot_id, str) or not boot_id:
        msg = "authority payload owner must carry a host boot identity"
        raise AuthorityError(msg)
    return AuthorityOwner(pid=pid, start_time_ticks=ticks, boot_id=boot_id)


def _check_non_negative_int(name: str, value: object) -> int:
    """Validate a non-negative integer payload field.

    Args:
        name: Field name for error context.
        value: Raw field value.

    Returns:
        The validated integer.

    Raises:
        AuthorityError: If the value is not a non-negative integer.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        msg = f"authority payload {name} must be a non-negative integer"
        raise AuthorityError(msg)
    return value


def _check_positive_int(name: str, value: object) -> int:
    """Validate a positive integer payload field.

    Args:
        name: Field name for error context.
        value: Raw field value.

    Returns:
        The validated integer.

    Raises:
        AuthorityError: If the value is not a positive integer.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        msg = f"authority payload {name} must be a positive integer"
        raise AuthorityError(msg)
    return value


def _neutral_payload(server: str) -> str:
    """Serialize the neutral initial payload: no owner, zero epoch.

    Args:
        server: Exact execution-server identity the row governs.

    Returns:
        Canonical payload text.
    """
    return canonical_row_text(
        AuthorityRow(server=_require_server_identity(server), epoch=0, generation=0, owner=None)
    )


def read_authority(conn: JobsConnection, server: str) -> AuthorityRow | None:
    """Read and validate the canonical authority row.

    Reads need no local allocation.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity.

    Returns:
        The validated row, or ``None`` when no row exists yet.

    Raises:
        AuthorityUnavailableError: If the database cannot be reached.

    Note:
        A present but corrupt or untrusted row fails closed via
        :class:`AuthorityError` from :func:`parse_authority_payload`.
    """
    row_id = authority_row_id(server)
    try:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(_READ_ROW_SQL, {"id": str(row_id)})
            fetched = cursor.fetchone()
    except psycopg.Error as exc:
        msg = f"lifecycle authority for server {server!r} is unreachable"
        raise AuthorityUnavailableError(msg) from exc
    if fetched is None:
        return None
    return parse_authority_payload(fetched[0], server=server)


def bootstrap_authority(conn: JobsConnection, server: str) -> AuthorityRow:
    """Converge concurrent bootstraps onto one canonical authority row.

    Each contender attempts ``INSERT ... ON CONFLICT DO NOTHING`` with the
    neutral initial payload, then reads the row: exactly one insert wins and
    every contender proceeds on the same row, so two daemons can never CAS
    different rows and both believe they own the fencing epoch.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity.

    Returns:
        The validated canonical row.

    Raises:
        AuthorityUnavailableError: If the database cannot be reached or the
            row vanishes mid-bootstrap.

    Note:
        A converged but corrupt or untrusted row fails closed via
        :class:`AuthorityError` from :func:`parse_authority_payload`.
    """
    row_id = authority_row_id(server)
    try:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                _BOOTSTRAP_INSERT_SQL, {"id": str(row_id), "payload": _neutral_payload(server)}
            )
            cursor.execute(_READ_ROW_SQL, {"id": str(row_id)})
            fetched = cursor.fetchone()
    except psycopg.Error as exc:
        msg = f"lifecycle authority for server {server!r} is unreachable"
        raise AuthorityUnavailableError(msg) from exc
    if fetched is None:
        msg = f"lifecycle authority row for server {server!r} vanished mid-bootstrap"
        raise AuthorityUnavailableError(msg)
    return parse_authority_payload(fetched[0], server=server)


def take_authority(conn: JobsConnection, server: str, owner: AuthorityOwner) -> AuthorityRow | None:
    """Take fencing-epoch ownership with a true compare-and-swap row update.

    When the row already names this exact incarnation the take is an
    idempotent adoption. Otherwise the epoch is bumped exactly when the row
    still holds byte-for-byte the observed state: the full-state guard
    makes a stale contender's update fail, so exactly one contender's
    commit wins and the loser observes the mismatch and stands down
    without spawning — and no interleaving can clobber a concurrently
    committed spawn obligation. A committed pre-spawn obligation survives
    the take so the new owner must still resolve the first spawn's fate
    before spawning a replacement.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity.
        owner: Exact incarnation taking ownership.

    Returns:
        The updated row on success, or ``None`` when a concurrent contender
        won the race first.

    Raises:
        AuthorityError: If no authority row exists to take.

    Note:
        A corrupt or untrusted row fails closed via :class:`AuthorityError`
        from :func:`read_authority`. Database outages surface as
        :class:`AuthorityUnavailableError` from the row helpers.
    """
    expected, current = _read_observed(conn, server)
    if current is None:
        msg = f"no lifecycle authority row for server {server!r}; bootstrap first"
        raise AuthorityError(msg)
    if current.owner == owner:
        return current
    updated = replace(current, epoch=current.epoch + 1, owner=owner)
    if not _compare_and_swap(conn, server, expected, canonical_row_text(updated)):
        return None
    return updated


def _read_observed(conn: JobsConnection, server: str) -> tuple[str, AuthorityRow | None]:
    """Read the stored payload text and the parsed row together.

    The stored text verbatim is the exact-state guard for a later
    compare-and-swap. Only exact canonical v2 rows parse here; a v1 or
    otherwise non-canonical row fails closed via :class:`AuthorityError`
    and must go through :func:`migrate_v1_to_v2` first.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity.

    Returns:
        The ``(stored text, parsed row)`` pair; the text is ``""`` and
        the row ``None`` when no row exists yet.

    Raises:
        AuthorityUnavailableError: If the database cannot be reached.

    Note:
        A present but corrupt or untrusted row fails closed via
        :class:`AuthorityError` from :func:`parse_authority_payload`.
    """
    row_id = authority_row_id(server)
    try:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(_READ_ROW_SQL, {"id": str(row_id)})
            fetched = cursor.fetchone()
    except psycopg.Error as exc:
        msg = f"lifecycle authority for server {server!r} is unreachable"
        raise AuthorityUnavailableError(msg) from exc
    if fetched is None:
        return "", None
    stored = fetched[0]
    if not isinstance(stored, str):
        stored = json.dumps(stored, sort_keys=True)
    return stored, parse_authority_payload(stored, server=server)


def _compare_and_swap(conn: JobsConnection, server: str, expected: str, candidate: str) -> bool:
    """Commit a candidate payload only when the row is still the observed state.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity, for error context.
        expected: Exact payload text the decision was based on.
        candidate: Payload text to commit.

    Returns:
        ``True`` when the update committed, ``False`` when a concurrent
        mutation won the race first.

    Raises:
        AuthorityUnavailableError: If the database cannot be reached.
    """
    try:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                _CAS_ROW_SQL,
                {
                    "id": str(authority_row_id(server)),
                    "payload": candidate,
                    "expected": expected,
                },
            )
            won = cursor.rowcount == 1
    except psycopg.Error as exc:
        msg = f"lifecycle authority for server {server!r} is unreachable"
        raise AuthorityUnavailableError(msg) from exc
    return won


def _claim_owner(claim: AuthorityClaim, current: AuthorityRow) -> AuthorityOwner:
    """Verify a fencing claim against an observed row and return its owner.

    Args:
        claim: In-memory fencing-epoch holding to verify.
        current: The freshly observed row.

    Returns:
        The row's exact owner.

    Raises:
        AuthorityError: If the row's epoch or owner no longer matches the
            claim: without a fresh match the action does not happen.
    """
    if current.epoch != claim.epoch or current.owner is None:
        msg = "lifecycle authority fencing epoch no longer matches the claim; holding"
        raise AuthorityError(msg)
    owner = current.owner
    if (
        owner.pid != claim.pid
        or owner.start_time_ticks != claim.start_time_ticks
        or owner.boot_id != claim.boot_id
    ):
        msg = "lifecycle authority fencing epoch no longer matches the claim; holding"
        raise AuthorityError(msg)
    return owner


def commit_spawn_obligation(
    conn: JobsConnection, claim: AuthorityClaim, spawn: SpawnObligation
) -> bool:
    """Commit a fenced pre-spawn obligation before the spawn syscall.

    The obligation is the crash-durable authority forbidding any successor
    from starting a second consumer beside a possibly-live first spawn. The
    update commits only when the row still holds byte-for-byte the observed
    state, so a superseded incarnation's commit — or any commit racing a
    concurrent mutation — fails and the spawn does not happen. When the
    commit fails (database unreachable), the spawn does not happen.

    Args:
        conn: Open database connection.
        claim: In-memory fencing-epoch holding authorizing the spawn.
        spawn: The pre-spawn obligation to commit.

    Returns:
        ``True`` when the obligation committed, ``False`` when a
        concurrent contender won the race first (the caller stands down
        without spawning).

    Raises:
        AuthorityError: If no authority row exists or the claim no longer
            matches the row.

    Note:
        Database outages surface as :class:`AuthorityUnavailableError`
        from the row helpers.
    """
    expected, current = _read_observed(conn, claim.server)
    if current is None:
        msg = f"no lifecycle authority row for server {claim.server!r}; bootstrap first"
        raise AuthorityError(msg)
    owner = _claim_owner(claim, current)
    if current.spawn is not None or current.worker is not None:
        return False
    updated = replace(current, owner=owner, spawn=spawn, worker=None)
    return _compare_and_swap(conn, claim.server, expected, canonical_row_text(updated))


def clear_spawn_obligation(conn: JobsConnection, claim: AuthorityClaim) -> bool:
    """Clear a committed pre-spawn obligation under the fencing epoch.

    Args:
        conn: Open database connection.
        claim: In-memory fencing-epoch holding owning the obligation.

    Returns:
        ``True`` when no database obligation remains (including when the
        row already holds none), ``False`` when the row changed under the
        decision and the caller must re-observe instead of assuming.

    Note:
        Database outages surface as :class:`AuthorityUnavailableError`
        from the row helpers.
    """
    try:
        expected, current = _read_observed(conn, claim.server)
    except AuthorityError:
        return False
    if current is None:
        return False
    try:
        owner = _claim_owner(claim, current)
    except AuthorityError:
        return False
    if current.spawn is None:
        return True
    updated = replace(current, owner=owner, spawn=None)
    return _compare_and_swap(conn, claim.server, expected, canonical_row_text(updated))


def publish_worker(
    conn: JobsConnection,
    claim: AuthorityClaim,
    *,
    spawn_token: str,
    worker: WorkerRecord,
) -> bool:
    """Atomically publish the maintained worker and clear its obligation.

    Publication and obligation clearance commit in one row update over the
    exact observed state: no observer ever sees a published worker beside
    an outstanding obligation for it, and no concurrent mutation — a stale
    takeover, a second commit, a fencing supersession — can slip between
    the two. The update commits only when the row still names the claim's
    epoch and owner and the outstanding obligation carries ``spawn_token``.

    Args:
        conn: Open database connection.
        claim: In-memory fencing-epoch holding owning the obligation.
        spawn_token: Lifecycle token of the obligation being published.
        worker: The exact published worker identity.

    Returns:
        ``True`` when the worker published, ``False`` when the row changed
        under the decision (the caller must re-observe and, with a live
        unpublished child in hand, converge it rather than assume).

    Raises:
        AuthorityError: If no authority row exists.

    Note:
        Database outages surface as :class:`AuthorityUnavailableError`
        from the row helpers.
    """
    expected, current = _read_observed(conn, claim.server)
    if current is None:
        msg = f"no lifecycle authority row for server {claim.server!r}; bootstrap first"
        raise AuthorityError(msg)
    try:
        owner = _claim_owner(claim, current)
    except AuthorityError:
        return False
    if current.spawn is None or current.spawn.token != spawn_token:
        return False
    if current.worker is not None:
        return False
    if worker.token != spawn_token:
        msg = "published worker identity must name the obligated incarnation"
        raise AuthorityError(msg)
    updated = replace(current, owner=owner, spawn=None, worker=worker)
    return _compare_and_swap(conn, claim.server, expected, canonical_row_text(updated))


def clear_worker(conn: JobsConnection, claim: AuthorityClaim, token: str) -> bool:
    """Clear a published worker record under the fencing epoch.

    Args:
        conn: Open database connection.
        claim: In-memory fencing-epoch holding owning the record.
        token: Lifecycle token of the published worker to clear.

    Returns:
        ``True`` when no worker record remains (including when the row
        already holds none), ``False`` when the row changed under the
        decision and the caller must re-observe instead of assuming.

    Note:
        Database outages surface as :class:`AuthorityUnavailableError`
        from the row helpers.
    """
    try:
        expected, current = _read_observed(conn, claim.server)
    except AuthorityError:
        return False
    if current is None:
        return False
    try:
        owner = _claim_owner(claim, current)
    except AuthorityError:
        return False
    if current.worker is None:
        return True
    if current.worker.token != token:
        return False
    updated = replace(current, owner=owner, worker=None)
    return _compare_and_swap(conn, claim.server, expected, canonical_row_text(updated))


def confirm_authority(conn: JobsConnection, claim: AuthorityClaim) -> bool:
    """Confirm an in-memory claim against a fresh canonical row read.

    A mismatch, a corrupt row, or an absent row reads as ``False``: without
    a fresh match the action does not happen. An unreachable database is
    deliberately *not* folded into ``False`` so callers can tell a
    connection-level outage (discard the connection, retry on a fresh one)
    apart from a genuine fencing mismatch (stand down on the same
    connection).

    Args:
        conn: Open database connection.
        claim: In-memory fencing-epoch holding to confirm.

    Returns:
        ``True`` only when a fresh row read names the same server, epoch,
        and exact owner as the claim.

    Note:
        A corrupt row fails closed via :class:`AuthorityError` from
        :func:`read_authority`, which is absorbed here into ``False``.
    """
    try:
        row = read_authority(conn, claim.server)
    except AuthorityError:
        return False
    if row is None:
        return False
    if row.epoch != claim.epoch:
        return False
    owner = row.owner
    if owner is None:
        return False
    return (
        owner.pid == claim.pid
        and owner.start_time_ticks == claim.start_time_ticks
        and owner.boot_id == claim.boot_id
    )


@dataclass(frozen=True, slots=True)
class V1LegacyEvidence:
    """Legacy local state transferred into v2 before the migration CAS.

    Only exact, unambiguous evidence is transferred; anything malformed or
    ambiguous fails closed and leaves the v1 row untouched.

    Attributes:
        live_worker: Exact live maintained-worker identity to publish, or
            ``None`` when no legacy meta proves one.
        worker_ambiguous: ``True`` when legacy worker evidence exists but
            cannot be proved exact and safe.
        spawn: Blocking pre-spawn/unresolved obligation identity to carry,
            or ``None`` when none is proved.
        spawn_ambiguous: ``True`` when legacy blocking evidence exists but
            cannot be proved exact and safe.
    """

    live_worker: WorkerRecord | None = None
    worker_ambiguous: bool = False
    spawn: SpawnObligation | None = None
    spawn_ambiguous: bool = False


def _parse_v1_payload(data: object, *, server: str) -> AuthorityRow:
    """Parse an exact v1 payload. Migration boundary only.

    Accepts the legacy shape where ``spawn``/``worker`` keys may be absent;
    every other field validates exactly as v2.

    Args:
        data: Stored payload text or decoded mapping at version 1.
        server: Exact execution-server identity the row must govern.

    Returns:
        The parsed authority row.

    Raises:
        AuthorityError: If the payload is not an exact v1 row.
    """
    if isinstance(data, str):
        if len(data.encode("utf-8")) > AUTHORITY_MAX_BYTES:
            msg = "authority payload exceeds the documented byte bound"
            raise AuthorityError(msg)
        try:
            data = json.loads(data)
        except json.JSONDecodeError as exc:
            msg = f"authority payload is not valid JSON: {exc}"
            raise AuthorityError(msg) from exc
    if not isinstance(data, dict):
        msg = "authority payload must be a JSON object"
        raise AuthorityError(msg)
    version = data.get("v")
    if version != LEGACY_AUTHORITY_VERSION or isinstance(version, bool):
        msg = f"not a v1 lifecycle authority row: {version!r}"
        raise AuthorityError(msg)
    if data.get("type") != AUTHORITY_TYPE:
        msg = f"payload is not lifecycle authority: {data.get('type')!r}"
        raise AuthorityError(msg)
    if data.get("server") != server:
        msg = "lifecycle authority row governs a different server; holding"
        raise AuthorityError(msg)
    epoch = _check_non_negative_int("epoch", data.get("epoch"))
    generation = _check_non_negative_int("generation", data.get("generation"))
    owner = _parse_owner(data.get("owner"))
    spawn = _parse_spawn(data.get("spawn"))
    worker = _parse_worker(data.get("worker"))
    if spawn is not None and worker is not None:
        msg = "authority row cannot carry both a spawn obligation and a published worker"
        raise AuthorityError(msg)
    return AuthorityRow(
        server=server, epoch=epoch, generation=generation, owner=owner, spawn=spawn, worker=worker
    )


def _check_migration_evidence(proven: V1LegacyEvidence) -> None:
    """Reject malformed or ambiguous legacy evidence before any row read.

    Args:
        proven: Legacy local evidence to transfer.

    Raises:
        AuthorityError: If safety cannot be proved from the evidence.
    """
    if proven.worker_ambiguous or proven.spawn_ambiguous:
        msg = "legacy local authority is malformed or ambiguous; holding on v1"
        raise AuthorityError(msg)
    if proven.live_worker is not None and proven.spawn is not None:
        msg = "legacy local authority names both a live worker and a blocking spawn; holding on v1"
        raise AuthorityError(msg)


def _resolve_migration_sections(
    legacy: AuthorityRow, proven: V1LegacyEvidence
) -> tuple[SpawnObligation | None, WorkerRecord | None]:
    """Combine a v1 row with proven legacy evidence into v2 sections.

    Args:
        legacy: The parsed v1 row.
        proven: Validated legacy local evidence.

    Returns:
        The ``(spawn, worker)`` pair for the v2 candidate.

    Raises:
        AuthorityError: If the row and the evidence disagree or both
            sections would be set.
    """
    spawn = legacy.spawn if legacy.spawn is not None else proven.spawn
    worker = legacy.worker if legacy.worker is not None else proven.live_worker
    if legacy.spawn is not None and proven.spawn is not None and legacy.spawn != proven.spawn:
        msg = "legacy row and local evidence disagree on the blocking spawn; holding on v1"
        raise AuthorityError(msg)
    if (
        legacy.worker is not None
        and proven.live_worker is not None
        and legacy.worker != proven.live_worker
    ):
        msg = "legacy row and local evidence disagree on the published worker; holding on v1"
        raise AuthorityError(msg)
    if spawn is not None and worker is not None:
        msg = "migration cannot carry both a spawn obligation and a published worker"
        raise AuthorityError(msg)
    return spawn, worker


def migrate_v1_to_v2(
    conn: JobsConnection, server: str, evidence: V1LegacyEvidence | None = None
) -> bool:
    """Migrate one exact v1 row to canonical v2 atomically. Startup boundary only.

    Normal runtime code must never call this: it is the single one-time
    compatibility boundary. It parses v1 only here, transfers unambiguous
    legacy local blocking/ownership identity into the v2 candidate, then
    compare-and-swaps the exact observed v1 text to v2. No local file is
    modified and no child is signalled; after the CAS the normal v2
    reconciler retires or recovers whatever was transferred.

    Crash semantics: a crash before the CAS leaves v1 untouched; a crash
    after the CAS leaves enough v2 DB state to prevent any duplicate spawn.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity.
        evidence: Unambiguous legacy local evidence to transfer. ``None``
            means no legacy state (neutral migration).

    Returns:
        ``True`` when the row is v2 afterwards (already v2 or just
        migrated), ``False`` when a concurrent mutation won the race and
        the caller must re-observe.

    Raises:
        AuthorityError: If the stored row is neither exact v2 nor exact v1,
            or the legacy evidence is malformed/ambiguous and safety cannot
            be proved. The v1 row is left untouched in every failure.
        AuthorityUnavailableError: If the database cannot be reached.
    """
    proven = evidence if evidence is not None else V1LegacyEvidence()
    _check_migration_evidence(proven)
    row_id = authority_row_id(server)
    try:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(_READ_ROW_SQL, {"id": str(row_id)})
            fetched = cursor.fetchone()
    except psycopg.Error as exc:
        msg = f"lifecycle authority for server {server!r} is unreachable"
        raise AuthorityUnavailableError(msg) from exc
    if fetched is None:
        msg = f"no lifecycle authority row for server {server!r}; bootstrap first"
        raise AuthorityError(msg)
    stored = fetched[0]
    if not isinstance(stored, str):
        stored = json.dumps(stored, sort_keys=True)
    try:
        parse_authority_payload(stored, server=server)
    except AuthorityError:
        pass
    else:
        return True
    legacy = _parse_v1_payload(stored, server=server)
    spawn, worker = _resolve_migration_sections(legacy, proven)
    candidate = canonical_row_text(
        AuthorityRow(
            server=server,
            epoch=legacy.epoch,
            generation=legacy.generation,
            owner=legacy.owner,
            spawn=spawn,
            worker=worker,
        )
    )
    return _compare_and_swap(conn, server, stored, candidate)
