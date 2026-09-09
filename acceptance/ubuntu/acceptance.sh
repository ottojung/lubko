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

# -- 3. cli/current points to exact source HEAD ----------------------------

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

# -- 4. Canonical pytest budget check (hard 10 s, no softening) -------------

printf '\n%s\n' '--- Canonical pytest budget check ---'
cd "$REPO"
if uv run python scripts/check_test_budget.py; then
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
