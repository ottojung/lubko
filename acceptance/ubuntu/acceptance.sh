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

# -- 3. Startup boundary invocation with supervisor state token ----------------

printf '\n%s\n' '--- Startup boundary invocation ---'

printf '%s\n' '  startup contract artifacts installed correctly'
if lubko-deploy startup-contract >/dev/null 2>&1; then
  pass "lubko-deploy startup-contract"
else
  fail "lubko-deploy startup-contract"
fi

printf '%s\n' '  valid token: supervisor crosses startup, creates pidfile + status'
VALID_TOKEN="aabbccddee00112233445566778899aabbccddee00112233445566778899aabb"
export LUBKO_SUPERVISOR_STATE_TOKEN="${VALID_TOKEN}"
lubko-supervisor &
SUPERVISOR_PID=$!
# Wait up to 8 seconds for pidfile to appear (supervisor writes it after
# acquiring the ownership lock, writing pidfile, persisting runtime commit,
# and opening the control socket — all purely filesystem-based).
WAIT=0
while [ "$WAIT" -lt 8 ]; do
  if [ -f "${STATE_ROOT}/supervisor/supervisor.pid" ]; then
    break
  fi
  sleep 1
  WAIT=$((WAIT + 1))
done
PIDFILE="${STATE_ROOT}/supervisor/supervisor.pid"
STATUSFILE="${STATE_ROOT}/supervisor/status.json"
if [ ! -f "$PIDFILE" ]; then
  fail "supervisor pidfile not created within 8 s"
else
  SVPID=$(cat "$PIDFILE" | python3 -c "import sys,json; print(json.load(sys.stdin)['pid'])")
  if [ "$SVPID" = "$SUPERVISOR_PID" ]; then
    pass "supervisor pidfile records correct PID ${SVPID}"
  else
    fail "supervisor pidfile PID ${SVPID} != actual ${SUPERVISOR_PID}"
  fi
fi
if [ ! -f "$STATUSFILE" ]; then
  fail "supervisor status.json not created within 8 s"
else
  pass "supervisor status.json created"
fi
# Clean shutdown: SIGTERM, wait up to 5 seconds for exit.
kill -TERM "$SUPERVISOR_PID" 2>/dev/null || true
WAIT=0
while [ "$WAIT" -lt 5 ]; do
  if ! kill -0 "$SUPERVISOR_PID" 2>/dev/null; then
    break
  fi
  sleep 1
  WAIT=$((WAIT + 1))
done
if kill -0 "$SUPERVISOR_PID" 2>/dev/null; then
  kill -KILL "$SUPERVISOR_PID" 2>/dev/null || true
  wait "$SUPERVISOR_PID" 2>/dev/null || true
  fail "supervisor did not exit within 5 s of SIGTERM"
else
  wait "$SUPERVISOR_PID" 2>/dev/null || true
  pass "supervisor exited cleanly after SIGTERM"
fi
unset LUBKO_SUPERVISOR_STATE_TOKEN

printf '%s\n' '  absent token: supervisor fails closed immediately'
unset LUBKO_SUPERVISOR_STATE_TOKEN || true
if lubko-supervisor 2>/dev/null; then
  fail "lubko-supervisor should fail without token"
else
  EXIT_CODE=$?
  if [ "$EXIT_CODE" -eq 1 ]; then
    pass "lubko-supervisor exit code 1 without token"
  else
    fail "lubko-supervisor exit code ${EXIT_CODE} (expected 1) without token"
  fi
fi

printf '%s\n' '  invalid token: supervisor fails closed immediately'
export LUBKO_SUPERVISOR_STATE_TOKEN="not-a-valid-hex-token"
if lubko-supervisor 2>/dev/null; then
  fail "lubko-supervisor should fail with invalid token"
else
  EXIT_CODE=$?
  if [ "$EXIT_CODE" -eq 1 ]; then
    pass "lubko-supervisor exit code 1 with invalid token"
  else
    fail "lubko-supervisor exit code ${EXIT_CODE} (expected 1) with invalid token"
  fi
fi
unset LUBKO_SUPERVISOR_STATE_TOKEN

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
