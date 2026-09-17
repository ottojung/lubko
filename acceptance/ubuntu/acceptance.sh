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

# -- 3. Startup contract boundary with supervisor state token ----------------

printf '\n%s\n' '--- Startup contract boundary ---'

printf '%s\n' '  lubko-deploy startup-contract validation'
if lubko-deploy startup-contract >/dev/null 2>&1; then
  pass "lubko-deploy startup-contract"
else
  fail "lubko-deploy startup-contract"
fi

printf '%s\n' '  contract definition includes required environment variable name'
if uv run python -c "
import json, sys
from lubko.startup_contract import generate_startup_definition
d = generate_startup_definition()
assert 'required_environment' in d, 'missing required_environment key'
assert 'LUBKO_SUPERVISOR_STATE_TOKEN' in d['required_environment'], \
    'LUBKO_SUPERVISOR_STATE_TOKEN not in required_environment'
print('OK: required_environment includes LUBKO_SUPERVISOR_STATE_TOKEN')
"; then
  pass "definition requires LUBKO_SUPERVISOR_STATE_TOKEN"
else
  fail "definition requires LUBKO_SUPERVISOR_STATE_TOKEN"
fi

printf '%s\n' '  launcher documents external token supply requirement'
if uv run python -c "
from lubko.startup_contract import generate_startup_launcher_content
c = generate_startup_launcher_content()
assert 'LUBKO_SUPERVISOR_STATE_TOKEN' in c, 'launcher missing token env name'
assert 'externally supplied' in c, 'launcher missing externally supplied note'
print('OK: launcher documents external token supply')
"; then
  pass "launcher documents external token supply"
else
  fail "launcher documents external token supply"
fi

printf '%s\n' '  valid 256-bit hex token passes validation'
if uv run python -c "
from lubko.state import validate_supervisor_state_token
tok = 'a' * 64
assert validate_supervisor_state_token(tok) == tok
print('OK: valid token accepted')
"; then
  pass "valid token accepted"
else
  fail "valid token accepted"
fi

printf '%s\n' '  absent token is detected'
if uv run python -c "
from lubko.state import supervisor_state_token
import os
os.environ.pop('LUBKO_SUPERVISOR_STATE_TOKEN', None)
assert supervisor_state_token() is None, 'expected None for absent token'
print('OK: absent token returns None')
"; then
  pass "absent token returns None"
else
  fail "absent token returns None"
fi

printf '%s\n' '  invalid tokens fail closed'
if uv run python -c "
from lubko.state import validate_supervisor_state_token, SupervisorStateTokenError
cases = [
    ('', 'empty'),
    ('short', 'too short'),
    ('g' + '0' * 63, 'non-hex char'),
    ('0' * 65, 'too long'),
]
for tok, desc in cases:
    try:
        validate_supervisor_state_token(tok)
        print(f'FAIL: {desc} should have been rejected')
        raise SystemExit(1)
    except SupervisorStateTokenError:
        pass
print('OK: all invalid tokens rejected')
"; then
  pass "invalid tokens fail closed"
else
  fail "invalid tokens fail closed"
fi

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
