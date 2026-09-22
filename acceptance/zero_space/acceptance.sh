#!/bin/sh
# Zero-space acceptance: an already-deployed Lubko operates with zero
# allocatable filesystem bytes.
#
# What this proves, end to end against a real PostgreSQL transport:
#   1. deploy (with space available) leaves a coherent deployed tree;
#   2. with zero allocation enforced, the supervisor restarts, takes
#      lifecycle authority over the database, and spawns its worker
#      (restart boundary #1);
#   3. a filesystem-write-free job publishes its exact stdout, stderr,
#      and result rows over the transport;
#   4. a second, subsequent job completes the same way (steady state,
#      not a one-shot);
#   5. a further supervisor/worker restart still works (restart boundary #2).
#
# How "zero allocatable bytes" is made explicit and verifiable:
# - The `zerospace` tracer (ptrace, syscall layer) fails every allocating
#   syscall under the Lubko-owned roots (state root, bin home, config home)
#   with ENOSPC and logs each denial with its path. Enforcement is purely
#   path-based: NO path is exempt as tmpfs, so the test never depends on
#   tmpfs or any spare filesystem. Reads, in-place rewrites, locks,
#   removals, pipes, sockets, and everything outside the roots pass through.
# - A self-test first proves the tracer denies (a probe create under the
#   roots must fail with ENOSPC while a control write outside succeeds)
#   and asserts the ACTIVE banner is present in the denial log.
# - A full file manifest (paths plus hashes) of the Lubko-owned roots is
#   taken before the zero phase and compared after it: any successful
#   unexpected write shows up as a diff with its exact path.
# - Every denial is classified: known-degraded diagnostics (status/health/
#   log/lock/interpreter-cache writes, which Lubko must survive) are
#   tallied, and any other Lubko-owned write attempt fails the run while
#   printing the exact offending lines.
#
# Faithful scope: the database is transport infrastructure (Supabase in
# production), so it necessarily lives outside the constrained roots on a
# writable filesystem; only Lubko-owned paths are constrained. The harness
# denial log and process stdio logs likewise live outside the roots.
#
# Environment (all optional):
#   ZERO_SPACE_REPO        Lubko checkout to deploy (default: this repo)
#   ZERO_SPACE_SCRATCH     harness scratch dir (default: mktemp -d)
#   ZERO_SPACE_CC          C compiler (default: cc, then gcc)
#   ZERO_SPACE_KEEP=1      keep the scratch tree on success (default: remove)
#   Superuser PostgreSQL for setup (CI service defaults shown):
#     ZERO_SPACE_SETUP_HOST/PORT/USER/PASSWORD/DBNAME
#   Application PostgreSQL (written into database.conf):
#     ZERO_SPACE_PG_HOST/PORT/DBNAME/USER/PASSWORD
#     ZERO_SPACE_SERVER    execution-server identity

set -eu

HERE="$(cd "$(dirname "$0")" && pwd -P)"
REPO="${ZERO_SPACE_REPO:-$(cd "${HERE}/../.." && pwd -P)}"
SERVER="${ZERO_SPACE_SERVER:-zero-space-acceptance}"

SETUP_HOST="${ZERO_SPACE_SETUP_HOST:-127.0.0.1}"
SETUP_PORT="${ZERO_SPACE_SETUP_PORT:-5432}"
SETUP_USER="${ZERO_SPACE_SETUP_USER:-postgres}"
SETUP_PASSWORD="${ZERO_SPACE_SETUP_PASSWORD:-postgres}"
SETUP_DBNAME="${ZERO_SPACE_SETUP_DBNAME:-postgres}"

PG_HOST="${ZERO_SPACE_PG_HOST:-127.0.0.1}"
PG_PORT="${ZERO_SPACE_PG_PORT:-5432}"
PG_DBNAME="${ZERO_SPACE_PG_DBNAME:-lubko_zero_space}"
PG_USER="${ZERO_SPACE_PG_USER:-lubko_zero_space}"
PG_PASSWORD="${ZERO_SPACE_PG_PASSWORD:-lubko_zero_space}"

FAILED=0

pass() { printf '  OK   %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILED=1; }
note() { printf '%s\n' "$1"; }

trap_cleanup() {
  # Best-effort teardown: never mask the recorded verdict.
  stop_supervised "cleanup" 2>/dev/null || true
  if [ "${ZERO_SPACE_KEEP:-0}" != "1" ] && [ "${FAILED}" = "0" ]; then
    rm -rf "${SCRATCH}"
  else
    note "scratch kept at: ${SCRATCH}"
  fi
}

children_of() {
  # Print child PIDs of $1, one per line.
  ps --ppid "$1" -o pid= 2>/dev/null | tr -s ' ' '\n' | grep -E '^[0-9]+$' || true
}

wait_gone() {
  # wait_gone PID TIMEOUT: true when PID disappears within TIMEOUT seconds.
  WAIT_PID="$1"
  WAIT_N="$2"
  WAIT_I=0
  while [ "${WAIT_I}" -lt "${WAIT_N}" ]; do
    if ! kill -0 "${WAIT_PID}" 2>/dev/null; then
      return 0
    fi
    sleep 1
    WAIT_I=$((WAIT_I + 1))
  done
  if kill -0 "${WAIT_PID}" 2>/dev/null; then
    return 1
  fi
  return 0
}

stop_supervised() {
  # Stop the traced supervisor (and, through it, the worker) cleanly.
  # $1 = phase label for messages.
  if [ ! -f "${SCRATCH}/run/tracer.pid" ]; then
    return 0
  fi
  TRACER_PID="$(cat "${SCRATCH}/run/tracer.pid")"
  if ! kill -0 "${TRACER_PID}" 2>/dev/null; then
    rm -f "${SCRATCH}/run/tracer.pid"
    return 0
  fi
  for SUP_PID in $(children_of "${TRACER_PID}"); do
    note "phase $1: SIGTERM supervisor ${SUP_PID}"
    kill -TERM "${SUP_PID}" 2>/dev/null || true
  done
  # The tracer exits once every tracee is reaped; the supervisor retires
  # its worker first, so tracer exit proves the whole boundary is down.
  if wait_gone "${TRACER_PID}" 60; then
    note "phase $1: supervisor/worker boundary is down"
  else
    note "phase $1: boundary did not stop; SIGKILL process group"
    kill -KILL "-${TRACER_PID}" 2>/dev/null || true
    wait_gone "${TRACER_PID}" 10 || true
    return 1
  fi
  rm -f "${SCRATCH}/run/tracer.pid"
  return 0
}

start_supervised() {
  # Start the supervisor under zero-allocation enforcement.
  # $1 = phase label (stdio goes to logs/sup-$1.log, outside the roots).
  rm -f "${SCRATCH}/run/tracer.pid"
  # shellcheck disable=SC2086
  setsid env ${ZERO_ENV} "${ZEROSPACE}" --roots "${ZERO_ROOTS}" \
    --log "${DENIAL_LOG}" -- lubko-supervisor \
    >"${SCRATCH}/logs/sup-$1.log" 2>&1 < /dev/null &
  echo "$!" > "${SCRATCH}/run/tracer.pid"
  note "phase $1: tracer pid $(cat "${SCRATCH}/run/tracer.pid")"
}

manifest() {
  # manifest ROOT... > file: path inventory with hashes and link targets.
  for MANIFEST_ROOT in "$@"; do
    find "${MANIFEST_ROOT}" | sort | while IFS= read -r ENTRY; do
      if [ -L "${ENTRY}" ]; then
        printf 'L %s -> %s\n' "${ENTRY}" "$(readlink "${ENTRY}")"
      elif [ -d "${ENTRY}" ]; then
        printf 'D %s\n' "${ENTRY}"
      elif [ -f "${ENTRY}" ]; then
        printf 'F %s %s\n' "${ENTRY}" "$(sha256sum <"${ENTRY}" | cut -d' ' -f1)"
      else
        printf 'S %s\n' "${ENTRY}"
      fi
    done
  done
}

printf '=== Lubko zero-space acceptance ===\n\n'

# -- 0. Harness prerequisites -------------------------------------------------

note '--- Harness prerequisites ---'
if [ ! -f "${REPO}/pyproject.toml" ]; then
  fail "repo checkout missing at ${REPO}"
fi
if [ -n "$(git -C "${REPO}" status --porcelain)" ]; then
  fail "repo checkout is dirty; deploy requires the exact committed code"
fi
command -v uv >/dev/null 2>&1 || fail "uv is not on PATH"
if [ ! -f "${HERE}/zerospace.c" ] || [ ! -f "${HERE}/drive.py" ]; then
  fail "acceptance sources missing under ${HERE}"
fi
if [ "${FAILED}" != "0" ]; then
  printf 'PREREQUISITES FAILED\n'
  exit 1
fi
pass "prerequisites"

# Queue-job identity must never leak from an outer Lubko run into the
# harness: deploy refuses to race a worker it thinks owns this process.
unset LUBKO_JOB_ID LUBKO_AGENT_ID LUBKO_INVOCATION_ID LUBKO_RUNNER_GEN \
  LUBKO_WORKER_ID LUBKO_PROMPT LUBKO_LIFECYCLE_TOKEN || true

SCRATCH="${ZERO_SPACE_SCRATCH:-}"
if [ -z "${SCRATCH}" ]; then
  SCRATCH="$(mktemp -d)"
fi
mkdir -p "${SCRATCH}/home" "${SCRATCH}/logs" "${SCRATCH}/run"
trap trap_cleanup EXIT INT TERM

# The harness scratch must support execution (tracer binary, scripts):
# a noexec mount fails fast here with a clear message instead of
# mysterious tracer deaths later.
printf '#!/bin/sh\nexit 3\n' >"${SCRATCH}/exec-probe.sh"
chmod 755 "${SCRATCH}/exec-probe.sh"
if "${SCRATCH}/exec-probe.sh"; then
  fail "exec probe unexpectedly succeeded"
else
  PROBE_RC=$?
  if [ "${PROBE_RC}" != "3" ]; then
    fail "scratch ${SCRATCH} cannot execute files (rc=${PROBE_RC}); pick an exec-capable filesystem"
  fi
fi
rm -f "${SCRATCH}/exec-probe.sh"
if [ "${FAILED}" != "0" ]; then
  printf 'PREREQUISITES FAILED\n'
  exit 1
fi
pass "exec-capable scratch at ${SCRATCH}"

export HOME="${SCRATCH}/home"
BIN_HOME="${HOME}/.local/bin"
STATE_ROOT="${HOME}/.local/state/lubko"
CONFIG_HOME="${HOME}/.config/lubko"
ZERO_ROOTS="${STATE_ROOT}:${BIN_HOME}:${CONFIG_HOME}"
DENIAL_LOG="${SCRATCH}/logs/denials.log"
ZEROSPACE="${SCRATCH}/zerospace"
# The constrained environment inherited by every traced process.
ZERO_ENV="HOME=${HOME} LUBKO_SUPERVISOR_STATE_TOKEN="

# -- 1. Build the tracer ------------------------------------------------------

note ''
note '--- Build zero-allocation tracer ---'
if [ -n "${ZERO_SPACE_CC:-}" ]; then
  CC_CANDIDATES="${ZERO_SPACE_CC}"
else
  CC_CANDIDATES="cc gcc"
fi
CC_BIN=""
for CANDIDATE in ${CC_CANDIDATES}; do
  if command -v "${CANDIDATE}" >/dev/null 2>&1; then
    CC_BIN="${CANDIDATE}"
    break
  fi
done
if [ -z "${CC_BIN}" ]; then
  fail "no C compiler (tried: ${CC_CANDIDATES})"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
if ! "${CC_BIN}" -O2 -Wall -Wextra -o "${ZEROSPACE}" "${HERE}/zerospace.c" 2>"${SCRATCH}/logs/cc.log"; then
  fail "tracer build failed:"
  cat "${SCRATCH}/logs/cc.log"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
if ! [ -x "${ZEROSPACE}" ]; then
  fail "tracer binary not executable"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "tracer built with ${CC_BIN}"

# -- 2. Transport setup (real PostgreSQL, outside the roots) -----------------

note ''
note '--- Transport setup ---'
if ! uv sync --frozen --project "${REPO}" >/dev/null 2>&1; then
  fail "uv sync --frozen failed for ${REPO}"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
VENV_PY="${REPO}/.venv/bin/python"
DRIVE="${VENV_PY} ${HERE}/drive.py"
if ! ${DRIVE} setup --setup-host "${SETUP_HOST}" --setup-port "${SETUP_PORT}" \
  --setup-user "${SETUP_USER}" --setup-password "${SETUP_PASSWORD}" \
  --setup-dbname "${SETUP_DBNAME}" --app-user "${PG_USER}" \
  --app-password "${PG_PASSWORD}" --app-dbname "${PG_DBNAME}"; then
  fail "database setup failed"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "transport database ${PG_DBNAME} ready"

# -- 3. Deploy with space available --------------------------------------------

note ''
note '--- Deploy (space available) ---'
mkdir -p "${CONFIG_HOME}"
chmod 700 "${CONFIG_HOME}"
printf 'host=%s\nport=%s\ndbname=%s\nuser=%s\npassword=%s\n' \
  "${PG_HOST}" "${PG_PORT}" "${PG_DBNAME}" "${PG_USER}" "${PG_PASSWORD}" \
  >"${CONFIG_HOME}/database.conf"
chmod 600 "${CONFIG_HOME}/database.conf"
printf 'server=%s\n' "${SERVER}" >"${CONFIG_HOME}/worker.conf"
chmod 600 "${CONFIG_HOME}/worker.conf"
export PATH="${BIN_HOME}:${PATH}"

if ! uv run --project "${REPO}" lubko-install --repo "${REPO}" >"${SCRATCH}/logs/install.log" 2>&1; then
  fail "lubko-install failed (see logs/install.log)"
  tail -5 "${SCRATCH}/logs/install.log" || true
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "lubko-install"
if ! lubko-deploy deploy --bootstrap --repo "${REPO}" >"${SCRATCH}/logs/deploy.log" 2>&1; then
  fail "lubko-deploy deploy --bootstrap failed (see logs/deploy.log)"
  tail -5 "${SCRATCH}/logs/deploy.log" || true
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "lubko-deploy deploy --bootstrap"

# The bootstrap leaves a directly spawned worker; the zero phase must start
# from a stopped boundary so every supervisor/worker start below crosses
# the restart boundary under enforcement.
DIRECT_PID="$(python3 -c "import json;print(json.load(open('${STATE_ROOT}/worker/meta.json'))['pid'])" 2>/dev/null || "${VENV_PY}" -c "import json;print(json.load(open('${STATE_ROOT}/worker/meta.json'))['pid'])" 2>/dev/null || true)"
if [ -n "${DIRECT_PID}" ] && kill -0 "${DIRECT_PID}" 2>/dev/null; then
  note "stopping bootstrap worker ${DIRECT_PID}"
  kill -TERM "${DIRECT_PID}" 2>/dev/null || true
  if ! wait_gone "${DIRECT_PID}" 30; then
    fail "bootstrap worker ${DIRECT_PID} did not stop"
    printf 'ACCEPTANCE FAILED\n'
    exit 1
  fi
fi
pass "bootstrap worker stopped"

# Pre-populate interpreter caches while space is available so the denial
# log records Lubko-owned writes, not interpreter cache noise.
CLI_PY="$(readlink "${STATE_ROOT}/cli/current")"
case "${CLI_PY}" in
/*) CLI_ENV="${CLI_PY}" ;;
*) CLI_ENV="${STATE_ROOT}/cli/${CLI_PY}" ;;
esac
"${CLI_ENV}/.venv/bin/python" -m compileall -q "${STATE_ROOT}" 2>/dev/null || true
pass "interpreter caches pre-populated"

# Supervisor state token: high-entropy, never generated by the product.
TOKEN="$("${VENV_PY}" -c "import secrets; print(secrets.token_hex(32))")"
export LUBKO_SUPERVISOR_STATE_TOKEN="${TOKEN}"
ZERO_ENV="HOME=${HOME} LUBKO_SUPERVISOR_STATE_TOKEN=${TOKEN}"

manifest "${STATE_ROOT}" "${BIN_HOME}" "${CONFIG_HOME}" >"${SCRATCH}/manifest.before"
note "deployed tree snapshot: $(wc -l <"${SCRATCH}/manifest.before") entries"
pass "deployed snapshot recorded"

# -- 4. Prove enforcement is active and explicit --------------------------------

note ''
note '--- Zero-allocation self-test ---'
rm -f "${DENIAL_LOG}"
if "${ZEROSPACE}" --roots "${ZERO_ROOTS}" --log "${DENIAL_LOG}" -- \
  sh -c "touch '${STATE_ROOT}/selftest-probe'" 2>/dev/null; then
  fail "self-test probe create unexpectedly succeeded (enforcement inactive?)"
else
  SELF_RC=$?
  if [ "${SELF_RC}" = "0" ]; then
    fail "self-test probe unexpectedly succeeded"
  fi
fi
if ! touch "${SCRATCH}/selftest-control" 2>/dev/null; then
  fail "control write outside the roots failed (harness broken)"
fi
rm -f "${SCRATCH}/selftest-control"
if ! grep -q "^ACTIVE roots=" "${DENIAL_LOG}" 2>/dev/null; then
  fail "denial log has no ACTIVE banner (enforcement unverifiable)"
fi
if ! grep -q "DENY open-create ${STATE_ROOT}/selftest-probe" "${DENIAL_LOG}" 2>/dev/null; then
  fail "denial log records no ENOSPC denial for the probe create"
  cat "${DENIAL_LOG}" 2>/dev/null || true
fi
if [ "${FAILED}" != "0" ]; then
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
rm -f "${DENIAL_LOG}"
pass "enforcement denies with ENOSPC; control path writable; banner verified"

# -- 5. Zero phase A: restart, authority, first job ------------------------------

note ''
note '--- Zero phase A: restart boundary, transport, first job ---'
start_supervised "a"
if ! ${DRIVE} ready --pg-host "${PG_HOST}" --pg-port "${PG_PORT}" \
  --pg-dbname "${PG_DBNAME}" --pg-user "${PG_USER}" \
  --pg-password "${PG_PASSWORD}" --server "${SERVER}"; then
  fail "phase A: supervisor never took lifecycle authority under zero allocation"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "phase A: restart boundary crossed, authority live (transport works)"

JOB1="$(${DRIVE} submit --pg-host "${PG_HOST}" --pg-port "${PG_PORT}" \
  --pg-dbname "${PG_DBNAME}" --pg-user "${PG_USER}" \
  --pg-password "${PG_PASSWORD}" --server "${SERVER}" \
  --marker zero-space-first)"
note "phase A: job1 ${JOB1}"
if ! ${DRIVE} await --pg-host "${PG_HOST}" --pg-port "${PG_PORT}" \
  --pg-dbname "${PG_DBNAME}" --pg-user "${PG_USER}" \
  --pg-password "${PG_PASSWORD}" --job-id "${JOB1}" \
  --marker zero-space-first; then
  fail "phase A: first job did not publish exact stdout/stderr/result"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "phase A: write-free job published exact stdout/stderr/result"

# -- 6. Zero phase B: second restart, second job ---------------------------------

note ''
note '--- Zero phase B: second restart, second job ---'
if ! stop_supervised "a-to-b"; then
  fail "phase A supervisor did not retire its worker on SIGTERM"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "phase A: clean shutdown retired the worker"
start_supervised "b"
if ! ${DRIVE} ready --pg-host "${PG_HOST}" --pg-port "${PG_PORT}" \
  --pg-dbname "${PG_DBNAME}" --pg-user "${PG_USER}" \
  --pg-password "${PG_PASSWORD}" --server "${SERVER}"; then
  fail "phase B: supervisor never re-took authority under zero allocation"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "phase B: second restart boundary crossed, authority live again"

JOB2="$(${DRIVE} submit --pg-host "${PG_HOST}" --pg-port "${PG_PORT}" \
  --pg-dbname "${PG_DBNAME}" --pg-user "${PG_USER}" \
  --pg-password "${PG_PASSWORD}" --server "${SERVER}" \
  --marker zero-space-second)"
note "phase B: job2 ${JOB2}"
if ! ${DRIVE} await --pg-host "${PG_HOST}" --pg-port "${PG_PORT}" \
  --pg-dbname "${PG_DBNAME}" --pg-user "${PG_USER}" \
  --pg-password "${PG_PASSWORD}" --job-id "${JOB2}" \
  --marker zero-space-second; then
  fail "phase B: second job did not publish exact stdout/stderr/result"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "phase B: subsequent job published exact stdout/stderr/result"

if ! stop_supervised "final"; then
  fail "final shutdown did not retire the worker"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "final clean shutdown"

# -- 7. Verdict: no successful writes, no unexpected attempts --------------------

note ''
note '--- Zero-allocation verdict ---'
manifest "${STATE_ROOT}" "${BIN_HOME}" "${CONFIG_HOME}" >"${SCRATCH}/manifest.after"
if ! diff -u "${SCRATCH}/manifest.before" "${SCRATCH}/manifest.after" >"${SCRATCH}/manifest.diff"; then
  fail "Lubko-owned tree changed under zero allocation:"
  cat "${SCRATCH}/manifest.diff"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "Lubko-owned tree byte-identical before/after the zero phase"

ACTIVE_COUNT="$(grep -c "^ACTIVE roots=" "${DENIAL_LOG}" || true)"
note "enforcement activations: ${ACTIVE_COUNT} (self-test + phase A + phase B)"
if [ "${ACTIVE_COUNT}" -lt 3 ]; then
  fail "expected at least 3 enforcement activations, saw ${ACTIVE_COUNT}"
fi

# Known-degraded diagnostics: Lubko must survive these denials, and they
# are the only Lubko-owned writes permitted to be attempted.
DENIED_TOTAL="$(grep -c "^DENY " "${DENIAL_LOG}" || true)"
note "total denied Lubko-owned writes survived: ${DENIED_TOTAL}"
UNEXPECTED="$(grep "^DENY " "${DENIAL_LOG}" | grep -v -E "DENY [a-z0-9-]+ ${STATE_ROOT}/(supervisor/status\.json|supervisor/supervisor\.log|worker/logs/|worker/health[^ ]*|worker/worker\.log)|DENY [a-z0-9-]+ ${STATE_ROOT}/.*__pycache__/.*|DENY [a-z0-9-]+ ${STATE_ROOT}/.*\.lubko-durable-lock-[^ ]*|DENY [a-z0-9-]+ ${BIN_HOME}/\.lubko-durable-lock-[^ ]*" || true)"
if [ -n "${UNEXPECTED}" ]; then
  fail "unexpected Lubko-owned writes attempted under zero allocation:"
  printf '%s\n' "${UNEXPECTED}"
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
pass "every denied write is a known-degraded diagnostic"

note ''
if [ "${FAILED}" -ne 0 ]; then
  printf 'ACCEPTANCE FAILED\n'
  exit 1
fi
printf 'ACCEPTANCE PASSED\n'
