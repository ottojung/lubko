#!/bin/sh
# Acceptance validation for a fresh Lubko installation on clean Ubuntu.
# Runs inside the Docker container after lubko-install.
set -eu

REPO=/src/lubko
BIN_HOME="${XDG_BIN_HOME:-${HOME}/.local/bin}"
FAILED=0

pass() { printf '  OK  %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILED=1; }

printf '=== Lubko Ubuntu acceptance ===\n\n'

# -- 1. Installed launcher existence and executability ----------------------

printf '--- Installed launchers ---\n'
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

# -- 2. CLI current symlink -------------------------------------------------

printf '\n--- CLI current symlink ---\n'
CURRENT="${XDG_STATE_HOME:-${HOME}/.local/state}/lubko/cli/current"
if [ ! -L "$CURRENT" ]; then
  fail "cli/current is not a symlink"
else
  TARGET="$(readlink "$CURRENT")"
  pass "cli/current -> $TARGET"
fi

# -- 3. Canonical pytest budget check (hard 10 s, no softening) -------------

printf '\n--- Canonical pytest budget check ---\n'
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
