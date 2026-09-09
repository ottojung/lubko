#!/bin/sh
# Acceptance validation for a fresh Lubko installation on Termux ARM64.
# Runs inside the termux/termux-docker:aarch64 container after build.
set -eu

REPO=/workspace
BIN_HOME="${XDG_BIN_HOME:-${HOME}/.local/bin}"
STATE_ROOT="${XDG_STATE_HOME:-${HOME}/.local/state}/lubko"
FAILED=0

pass() { printf '  OK  %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; FAILED=1; }

printf '=== Lubko Termux ARM64 acceptance ===\n\n'

# -- 0. Provision: Termux packages and XDG config --------------------------

printf '%s\n' '--- Termux packages ---'
for pkg in python uv git libpq libtermux-exec; do
  if command -v "$pkg" >/dev/null 2>&1 || \
     [ -f "/data/data/com.termux/files/usr/bin/$pkg" ]; then
    pass "$pkg installed"
  else
    fail "$pkg not found"
  fi
done

printf '\n%s\n' '--- Private XDG config ---'
mkdir -p "${HOME}/.config/lubko"
printf 'host=127.0.0.1\nport=5432\ndbname=lubko\nuser=lubko\npassword=lubko\n' \
    > "${HOME}/.config/lubko/database.conf"
chmod 600 "${HOME}/.config/lubko/database.conf"
printf 'server=acceptance-test\n' \
    > "${HOME}/.config/lubko/worker.conf"
chmod 600 "${HOME}/.config/lubko/worker.conf"
pass "XDG config created"

# -- 1. Frozen sync (real Termux uv, real lockfile) ------------------------

printf '\n%s\n' '--- Frozen sync ---'
cd "$REPO"
if uv sync --frozen; then
  pass "uv sync --frozen"
else
  fail "uv sync --frozen"
fi

# -- 2. lubko-install (real installation) ----------------------------------

printf '\n%s\n' '--- lubko-install ---'
export PATH="${BIN_HOME}:${PATH}"
if uv run lubko-install --repo "$REPO"; then
  pass "lubko-install"
else
  fail "lubko-install"
fi

# -- 3. Installed launcher existence and executability ----------------------

printf '\n%s\n' '--- Installed launchers ---'
for entry in lubko-agent lubko-worker lubko-supervisor lubko-deploy \
             lubko-deploy-ctl lubko-install my-lubko-agent; do
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

# -- 4. Execute installed launchers from /tmp (outside checkout) -----------

printf '\n%s\n' '--- Installed launcher execution (from /tmp) ---'
cd /tmp

if lubko-install --repo "$REPO" --dry-run >/dev/null 2>&1; then
  pass "lubko-install --dry-run"
else
  fail "lubko-install --dry-run"
fi

if lubko-agent --help >/dev/null 2>&1; then
  pass "lubko-agent --help"
else
  fail "lubko-agent --help"
fi

# -- 5. cli/current points to exact source HEAD ----------------------------

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

# -- 6. Canonical pytest budget check (hard 10 s) --------------------------

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
