#!/bin/sh
# Acceptance validation for a fresh Lubko installation on clean Ubuntu.
# Runs inside the Docker container after lubko-install.
set -eu

REPO=/src/lubko
BIN_HOME="${XDG_BIN_HOME:-${HOME}/.local/bin}"
STATE_ROOT="${XDG_STATE_HOME:-${HOME}/.local/state}/lubko"
FAILED=0

pass() { printf '  OK  %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILED=1; }

printf '=== Lubko Ubuntu acceptance ===\n\n'

# -- 1. Installed launcher existence and executability ----------------------

printf '%s\n' '--- Installed launchers ---'
for entry in lubko-agent lubko-worker lubko-supervisor lubko-deploy \
             lubko-deploy-ctl lubko-install my-lubko-agent lubko-startup; do
  path="${BIN_HOME}/${entry}"
  if [ ! -f "$path" ]; then
    fail "$entry: missing"
    continue
  fi
  if [ ! -x "$path" ]; then
    fail "$entry: not executable"
    continue
  fi
  pass "$entry"
done

# -- 2. Execute installed launchers from an unrelated working directory ------

printf '\n%s\n' '--- Installed launcher execution (from /tmp) ---'
export PATH="${BIN_HOME}:${PATH}"
cd /tmp

if lubko-install --repo "${REPO}" --dry-run >/dev/null 2>&1; then
  pass "lubko-install --dry-run"
else
  fail "lubko-install --dry-run"
fi

if lubko-agent --help >/dev/null 2>&1; then
  pass "lubko-agent --help"
else
  fail "lubko-agent --help"
fi

# -- 3. Startup boundary invocation via installed lubko-startup launcher ----

printf '\n%s\n' '--- Startup boundary invocation ---'

printf '%s\n' '  startup contract artifacts installed correctly'
if lubko-deploy startup-contract >/dev/null 2>&1; then
  pass "lubko-deploy startup-contract"
else
  fail "lubko-deploy startup-contract"
fi

# The supervisor is database-authoritative and fail-closed: with no
# reachable PostgreSQL it holds without writing the pidfile, which used
# to wedge this script in an unbounded teardown wait. Start the intended
# local PostgreSQL transport (role/db from database.conf plus the frozen
# transport schema) so the boundary crossing has something to reach.
# Supervisor semantics are untouched: without the database it still holds.
printf '%s\n' '  acceptance PostgreSQL transport reachable'
if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
  pass "postgres already accepting connections"
else
  service postgresql start >/dev/null 2>&1 || true
  if pg_isready -h 127.0.0.1 -p 5432 >/dev/null 2>&1; then
    pass "postgres started"
  else
    fail "postgres could not be started"
  fi
fi
if [ "$FAILED" -eq 0 ]; then
  su postgres -c "psql -v ON_ERROR_STOP=1 -tAc \"SELECT 1 FROM pg_roles WHERE rolname='lubko'\" | grep -q 1 || psql -v ON_ERROR_STOP=1 -c \"CREATE ROLE lubko LOGIN PASSWORD 'lubko'\"" >/dev/null 2>&1 || fail "create role lubko"
  su postgres -c "psql -v ON_ERROR_STOP=1 -tAc \"SELECT 1 FROM pg_database WHERE datname='lubko'\" | grep -q 1 || createdb -O lubko lubko" >/dev/null 2>&1 || fail "create database lubko"
  PGPASSWORD=lubko psql -h 127.0.0.1 -p 5432 -U lubko -d lubko -v ON_ERROR_STOP=1 -f "${REPO}/migrations/0001_two_column_protocol.sql" >/dev/null 2>&1 || fail "apply transport schema"
  if [ "$FAILED" -eq 0 ]; then
    pass "acceptance PostgreSQL transport ready"
  fi
fi

# A minimal tini-static shim so the installed lubko-startup launcher can
# execute its canonical `exec tini-static -- lubko-supervisor` chain.
TINI_DIR="${HOME}/.lubko-acceptance-tini"
mkdir -p "${TINI_DIR}"
cat > "${TINI_DIR}/tini-static" << 'SHIM'
#!/bin/sh
# Tini shim: consume the leading -- separator (tini static convention)
# then exec the remaining command and arguments.
shift
exec "$@"
SHIM
chmod 755 "${TINI_DIR}/tini-static"
export PATH="${TINI_DIR}:${PATH}"

printf '%s\n' '  valid token: lubko-startup crosses startup boundary'
VALID_TOKEN="aabbccddee00112233445566778899aabbccddee00112233445566778899aabb"
export LUBKO_SUPERVISOR_STATE_TOKEN="${VALID_TOKEN}"
lubko-startup &
STARTUP_PID=$!
# Wait up to 8 seconds for pidfile to appear as readiness proof. Stop
# early if the startup job itself exits (fail-closed), so a missing
# database can never wedge this wait past one extra poll.
WAIT=0
while [ "$WAIT" -lt 8 ]; do
  if [ -f "${STATE_ROOT}/supervisor/supervisor.pid" ]; then
    break
  fi
  if ! kill -0 "$STARTUP_PID" 2>/dev/null; then
    break
  fi
  sleep 1
  WAIT=$((WAIT + 1))
done
PIDFILE="${STATE_ROOT}/supervisor/supervisor.pid"
STATUSFILE="${STATE_ROOT}/supervisor/status.json"
PIDFILE_OK=1
if [ ! -f "$PIDFILE" ]; then
  PIDFILE_OK=0
  fail "supervisor pidfile not created within 8 s"
else
  pass "supervisor pidfile created"
fi
if [ ! -f "$STATUSFILE" ]; then
  fail "supervisor status.json not created within 8 s"
else
  pass "supervisor status.json created"
fi
# Bounded fail-loud teardown: SIGTERM the supervisor-recorded PID (when
# the pidfile exists) and the startup job itself, wait at most 5 s, then
# SIGKILL survivors. A missing pidfile stays a failure: it must never be
# reported as a clean exit, and the startup job must not outlive this block.
SVPID=$(sed -n 's/.*"pid": \([0-9]*\).*/\1/p' "$PIDFILE" 2>/dev/null || true)
if [ -n "${SVPID:-}" ]; then
  kill -TERM "$SVPID" 2>/dev/null || true
fi
kill -TERM "$STARTUP_PID" 2>/dev/null || true
WAIT=0
while [ "$WAIT" -lt 5 ]; do
  ALIVE=0
  if [ -n "${SVPID:-}" ] && kill -0 "$SVPID" 2>/dev/null; then
    ALIVE=1
  fi
  if kill -0 "$STARTUP_PID" 2>/dev/null; then
    ALIVE=1
  fi
  if [ "$ALIVE" -eq 0 ]; then
    break
  fi
  sleep 1
  WAIT=$((WAIT + 1))
done
if [ -n "${SVPID:-}" ] && kill -0 "$SVPID" 2>/dev/null; then
  kill -KILL "$SVPID" 2>/dev/null || true
  fail "supervisor did not exit within 5 s of SIGTERM"
fi
if kill -0 "$STARTUP_PID" 2>/dev/null; then
  kill -KILL "$STARTUP_PID" 2>/dev/null || true
  wait "$STARTUP_PID" 2>/dev/null || true
  fail "startup job did not exit within 5 s of SIGTERM"
else
  wait "$STARTUP_PID" 2>/dev/null || true
  if [ "$PIDFILE_OK" -eq 1 ]; then
    if [ -n "${SVPID:-}" ] && kill -0 "$SVPID" 2>/dev/null; then
      :
    else
      pass "supervisor exited cleanly after SIGTERM"
    fi
  fi
fi
rm -rf "${STATE_ROOT}/supervisor"
unset LUBKO_SUPERVISOR_STATE_TOKEN

printf '%s\n' '  absent token: lubko-startup fails closed immediately'
unset LUBKO_SUPERVISOR_STATE_TOKEN || true
if lubko-startup 2>/dev/null; then
  fail "lubko-startup should fail without token"
else
  EXIT_CODE=$?
  if [ "$EXIT_CODE" -eq 1 ]; then
    pass "lubko-startup exit code 1 without token"
  else
    fail "lubko-startup exit code ${EXIT_CODE} (expected 1) without token"
  fi
fi

printf '%s\n' '  invalid token: lubko-startup fails closed immediately'
export LUBKO_SUPERVISOR_STATE_TOKEN="not-a-valid-hex-token"
if lubko-startup 2>/dev/null; then
  fail "lubko-startup should fail with invalid token"
else
  EXIT_CODE=$?
  if [ "$EXIT_CODE" -eq 1 ]; then
    pass "lubko-startup exit code 1 with invalid token"
  else
    fail "lubko-startup exit code ${EXIT_CODE} (expected 1) with invalid token"
  fi
fi
unset LUBKO_SUPERVISOR_STATE_TOKEN

rm -rf "${TINI_DIR}"

# -- 4. cli/current points to exact source HEAD ----------------------------

printf '\n%s\n' '--- cli/current points to source HEAD ---'
CURRENT="${STATE_ROOT}/cli/current"
if [ ! -L "$CURRENT" ]; then
  fail "cli/current is not a symlink"
else
  TARGET="$(readlink "$CURRENT")"
  HEAD="$(git -C "${REPO}" rev-parse HEAD)"
  if [ "$TARGET" = "$HEAD" ]; then
    pass "cli/current == ${HEAD}"
  else
    fail "cli/current is ${TARGET}, expected ${HEAD}"
  fi
fi

# -- 5. Canonical pytest budget check (hard 10 s, no softening) -------------

printf '\n%s\n' '--- Canonical pytest budget check ---'
cd "$REPO"
if uv run pytest; then
  pass "pytest within 10 s budget"
else
  fail "pytest budget exceeded or tests failed"
fi

# -- Result -----------------------------------------------------------------

printf '\n'
if [ "$FAILED" -ne 0 ]; then
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
printf 'ACCEPTANCE PASSED\n'
