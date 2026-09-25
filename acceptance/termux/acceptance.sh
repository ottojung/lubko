#!/bin/sh
# Acceptance validation for a fresh Lubko installation on Termux ARM64.
# Runs inside the termux/termux-docker:aarch64 container as the system user.
#
# Phase 1 (runtime): provision only runtime prerequisites, plain frozen
#   sync, real lubko-install, launcher/current checks — proves the product
#   installs and runs on Termux without any compiler toolchain.
# Phase 1 and Phase 2 cover runtime installation and development/test validation.
#
# Phase 2 (dev/test): provision build tools, set ANDROID_API_LEVEL, frozen
#   sync with --extra dev, then the canonical pytest budget checker.
set -eu

REPO=/workspace
BIN_HOME="${XDG_BIN_HOME:-${HOME}/.local/bin}"
STATE_ROOT="${XDG_STATE_HOME:-${HOME}/.local/state}/lubko"
FAILED=0

pass() { printf '  OK  %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILED=1; }

printf '=== Lubko Termux ARM64 acceptance ===\n\n'

# -- Shared environment -----------------------------------------------------

export DEBIAN_FRONTEND=noninteractive

# libtermux-exec-ld-preload.so ships with the termux-docker image itself;
# fail loudly if the image ever stops providing it.
LD_PRELOAD_LIB="${PREFIX}/lib/libtermux-exec-ld-preload.so"
if [ ! -f "$LD_PRELOAD_LIB" ]; then
  fail "required library $LD_PRELOAD_LIB not found in image"
  exit 1
fi
pass "libtermux-exec-ld-preload.so present"
export LD_PRELOAD="$LD_PRELOAD_LIB"

# Termux lacks conventional /tmp; create a private outside-workspace dir.
LUBKO_OUTSIDE="${TMPDIR:-${HOME}/.cache/lubko-acceptance-tmp}"
mkdir -p "$LUBKO_OUTSIDE"
pass "outside-workspace dir $LUBKO_OUTSIDE"

printf '\n%s\n' '--- Private XDG config ---'
mkdir -p "${HOME}/.config/lubko"
printf 'host=127.0.0.1\nport=5432\ndbname=lubko\nuser=lubko\npassword=lubko\n' \
    > "${HOME}/.config/lubko/database.conf"
chmod 600 "${HOME}/.config/lubko/database.conf"
printf 'server=acceptance-test\n' \
    > "${HOME}/.config/lubko/worker.conf"
chmod 600 "${HOME}/.config/lubko/worker.conf"
pass "XDG config created"

# ===========================================================================
# PHASE 1 — Runtime installation (no compiler toolchain)
# ===========================================================================

printf '\n--- PHASE 1: Runtime installation ---\n\n'

printf '%s\n' '--- Runtime package install ---'
apt-get update -qq
apt-get -qq -y -o Dpkg::Options::=--force-confnew upgrade
apt-get install -qq -y -o Dpkg::Options::=--force-confnew \
    python uv git libpq

printf '%s\n' '--- Runtime versions ---'
python --version
uv --version
git --version
printf 'libpq: %s\n' "$(ls "${PREFIX}/lib/libpq.so"* 2>/dev/null | head -1 || echo 'not found')"

printf '\n%s\n' '--- Frozen sync (runtime only) ---'
cd "$REPO"
if uv sync --frozen; then
  pass "uv sync --frozen (runtime)"
else
  fail "uv sync --frozen (runtime)"
fi

printf '\n%s\n' '--- lubko-install ---'
export PATH="${BIN_HOME}:${PATH}"
if uv run lubko-install --repo "$REPO"; then
  pass "lubko-install"
else
  fail "lubko-install"
fi

printf '\n%s\n' '--- Installed launchers ---'
for entry in lubko-worker lubko-supervisor lubko-deploy \
             lubko-deploy-ctl lubko-install lubko-startup; do
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

printf '\n%s\n' '--- Installed launcher execution (outside checkout) ---'
cd "$LUBKO_OUTSIDE"

if lubko-install --repo "$REPO" --dry-run >/dev/null 2>&1; then
  pass "lubko-install --dry-run"
else
  fail "lubko-install --dry-run"
fi

printf '\n%s\n' '--- Startup boundary invocation ---'

printf '%s\n' '  startup contract artifacts installed correctly'
if lubko-deploy startup-contract >/dev/null 2>&1; then
  pass "lubko-deploy startup-contract"
else
  fail "lubko-deploy startup-contract"
fi

# The supervisor is database-authoritative and fail-closed: with no
# reachable PostgreSQL it holds without writing the pidfile. The official
# termux-docker environment cannot reliably host a PostgreSQL server itself
# (its Android shared-memory emulation is intentionally incomplete), so this
# acceptance connects to the external PostgreSQL service supplied by the
# GitHub Actions runner. That matches Lubko's production architecture: the
# transport is external infrastructure, not a service Termux must host.
printf '%s\n' '  external acceptance PostgreSQL transport reachable'
if REPO="$REPO" uv run --project "$REPO" python - <<'PY'
import os
from pathlib import Path

import psycopg

schema = (Path(os.environ["REPO"]) / "migrations" / "0001_two_column_protocol.sql").read_text(
    encoding="utf-8"
)
with psycopg.connect(
    host="127.0.0.1",
    port=5432,
    dbname="lubko",
    user="lubko",
    password="lubko",
    connect_timeout=5,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(schema)
print("acceptance PostgreSQL transport ready")
PY
then
  pass "acceptance PostgreSQL transport ready"
else
  fail "acceptance PostgreSQL transport setup failed"
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

# ===========================================================================
# PHASE 2 — Development/test environment
# ===========================================================================

printf '\n--- PHASE 2: Development/test environment ---\n\n'

printf '%s\n' '--- Dev package install ---'
apt-get install -qq -y -o Dpkg::Options::=--force-confnew \
    rust clang make cmake

# Termux native builds (maturin/ruff) require ANDROID_API_LEVEL.
# Termux packages use API level 24 (see termux-packages TERMUX_PKG_API_LEVEL).
export ANDROID_API_LEVEL=24

printf '\n%s\n' '--- Frozen sync (with dev extras) ---'
cd "$REPO"
if uv sync --frozen --extra dev; then
  pass "uv sync --frozen --extra dev"
else
  fail "uv sync --frozen --extra dev"
fi

# Re-run lubko-install after the dev sync so the startup contract is always
# written with the current code's CONTRACT_SCHEMA_VERSION, regardless of
# whether uv sync re-installed a cached lubko package layer.
printf '\n%s\n' '--- Re-install lubko after dev sync ---'
if uv run lubko-install --repo "$REPO"; then
  pass "lubko-install (post dev-sync)"
else
  fail "lubko-install (post dev-sync)"
fi

# -- Canonical pytest budget check (hard 10 s) ------------------------------

printf '\n%s\n' '--- Canonical pytest budget check ---'
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
