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
from dataclasses import dataclass
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
AUTHORITY_VERSION: Final = 1

#: Strict maximum size of the serialized authority payload in UTF-8 bytes.
#: Transitions that would exceed the bound are refused before any
#: irreversible action, exactly as deployments are refused today. No history
#: accumulates: the row holds current state only.
AUTHORITY_MAX_BYTES: Final = 4096

#: Fixed namespace binding authority row ids to execution-server names.
#: UUIDv5 over this namespace plus the exact server string derives the row
#: ``id`` deterministically in application code.
AUTHORITY_NAMESPACE: Final = UUID("24b16c63-7210-4b87-9bc6-09583150835d")

#: Totalizing payload-to-JSONB boundary mirroring
#: :func:`lubko.worker._safe_payload_sql`. Kept local so this module stays
#: free of the worker's import weight; the two expressions must stay
#: textually equivalent.
_AUTHORITY_JSON_SQL: Final = "((CASE WHEN payload IS JSON THEN payload END)::jsonb)"

#: Bootstrap insert converging concurrent first writers onto one row.
_BOOTSTRAP_INSERT_SQL: Final = (
    "INSERT INTO lubko.jobs (id, payload)\n"
    "VALUES (%(id)s, %(payload)s)\n"
    "ON CONFLICT (id) DO NOTHING"
)

#: Canonical row read by deterministic id.
_READ_ROW_SQL: Final = "SELECT payload FROM lubko.jobs\nWHERE id = %(id)s"

#: Ownership take guarded by a fencing-epoch compare-and-swap plus exact
#: kind and server predicates, so two contenders can never CAS different
#: rows and both believe they own the epoch.
_TAKE_ROW_SQL: Final = (
    "UPDATE lubko.jobs\n"
    "SET payload = %(payload)s\n"
    "WHERE id = %(id)s\n"
    f"    AND {_AUTHORITY_JSON_SQL}->>'type' = 'lifecycle_authority'\n"
    f"    AND {_AUTHORITY_JSON_SQL}->>'server' = %(server)s\n"
    f"    AND {_AUTHORITY_JSON_SQL}->>'epoch' = %(expected_epoch)s"
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
class AuthorityRow:
    """A parsed and validated lifecycle authority row."""

    server: str
    epoch: int
    generation: int
    owner: AuthorityOwner | None


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
    }


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
    epoch = _check_non_negative_int("epoch", data.get("epoch"))
    generation = _check_non_negative_int("generation", data.get("generation"))
    owner = _parse_owner(data.get("owner"))
    return AuthorityRow(server=server, epoch=epoch, generation=generation, owner=owner)


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
    return serialize_authority_payload(
        build_authority_payload(server=server, epoch=0, generation=0, owner=None)
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
    """Take fencing-epoch ownership with a compare-and-swap row update.

    When the row already names this exact incarnation the take is an
    idempotent adoption. Otherwise the epoch is bumped exactly when the row
    still carries the observed epoch: the guard predicate makes a concurrent
    bump fail the update, so exactly one contender's commit wins and the
    loser observes the mismatch and stands down without spawning.

    Args:
        conn: Open database connection.
        server: Exact execution-server identity.
        owner: Exact incarnation taking ownership.

    Returns:
        The updated row on success, or ``None`` when a concurrent contender
        won the epoch first.

    Raises:
        AuthorityError: If no authority row exists to take.
        AuthorityUnavailableError: If the database cannot be reached.

    Note:
        A corrupt or untrusted row fails closed via :class:`AuthorityError`
        from :func:`read_authority`.
    """
    current = read_authority(conn, server)
    if current is None:
        msg = f"no lifecycle authority row for server {server!r}; bootstrap first"
        raise AuthorityError(msg)
    if current.owner == owner:
        return current
    candidate = serialize_authority_payload(
        build_authority_payload(
            server=server,
            epoch=current.epoch + 1,
            generation=current.generation,
            owner=owner,
        )
    )
    try:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(
                _TAKE_ROW_SQL,
                {
                    "id": str(authority_row_id(server)),
                    "payload": candidate,
                    "server": server,
                    "expected_epoch": str(current.epoch),
                },
            )
            won = cursor.rowcount == 1
    except psycopg.Error as exc:
        msg = f"lifecycle authority for server {server!r} is unreachable"
        raise AuthorityUnavailableError(msg) from exc
    if not won:
        return None
    return AuthorityRow(
        server=server,
        epoch=current.epoch + 1,
        generation=current.generation,
        owner=owner,
    )


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
