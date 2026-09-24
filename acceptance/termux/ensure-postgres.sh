#!/bin/sh
# Ensure a reachable local PostgreSQL transport for Termux acceptance.
#
# The supervisor is database-authoritative and fail-closed: with no
# reachable PostgreSQL it holds without writing the pidfile, which wedged
# acceptance in an unbounded teardown wait. This helper provides the
# intended transport (role/db from database.conf plus the frozen transport
# schema) without touching supervisor semantics: without the database the
# supervisor still holds.
#
# Runs as an unprivileged user (PostgreSQL refuses initdb/postgres as
# root). The caller selects the user; this script uses $HOME of the
# invoking user for the private data directory.
set -eu

REPO="${REPO:-/workspace}"
PGDATA="${HOME}/.lubko-acceptance-pgdata"
PGLOG="${PGDATA}/acceptance-postgres.log"
PGSOCKET="${HOME}/.lubko-acceptance-pg-socket"
SCHEMA="${REPO}/migrations/0001_two_column_protocol.sql"

if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
  printf 'postgres already accepting connections\n'
  exit 0
fi

if [ ! -f "${PGDATA}/PG_VERSION" ]; then
  mkdir -p "$PGDATA"
  initdb -D "$PGDATA" -E UTF8 >/dev/null 2>&1
fi

mkdir -p "$PGSOCKET"
pg_ctl -D "$PGDATA" -l "$PGLOG" -o "-p 5432 -k $PGSOCKET" start >/dev/null 2>&1 || true

READY=0
i=0
while [ "$i" -lt 10 ]; do
  if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
  i=$((i + 1))
done
if [ "$READY" -ne 1 ]; then
  printf 'postgres did not become ready; see %s\n' "$PGLOG"
  exit 1
fi

psql -h 127.0.0.1 -p 5432 -d postgres -v ON_ERROR_STOP=1 \
  -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lubko') THEN CREATE ROLE lubko LOGIN PASSWORD 'lubko'; END IF; END \$\$;" >/dev/null 2>&1
if ! psql -h 127.0.0.1 -p 5432 -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='lubko'" 2>/dev/null | grep -q 1; then
  createdb -h 127.0.0.1 -p 5432 -O lubko lubko >/dev/null 2>&1
fi
PGPASSWORD=lubko psql -h 127.0.0.1 -p 5432 -U lubko -d lubko \
  -v ON_ERROR_STOP=1 -f "$SCHEMA" >/dev/null 2>&1

printf 'acceptance PostgreSQL transport ready\n'
