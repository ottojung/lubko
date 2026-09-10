#!/bin/sh
# Acceptance validation for a fresh Lubko installation on Termux ARM64.
# Runs inside the termux/termux-docker:aarch64 container as the system user.
#
# Phase 1 (runtime): provision only runtime prerequisites, plain frozen
#   sync, real lubko-install, launcher/current checks — proves the product
#   installs and runs on Termux without any compiler toolchain.
#
# Phase 1b (opencode): download the pinned native ARM64 OpenCode artifact,
#   verify SHA256, install to PATH, prove version and clean exit.  This is
#   an agent-backend dependency kept separate from Lubko runtime prerequisites.
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

printf '\n%s\n' '--- Installed launcher execution (outside checkout) ---'
cd "$LUBKO_OUTSIDE"

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
# PHASE 1b — OpenCode native ARM64 artifact (agent backend, not Lubko runtime)
# ===========================================================================

printf '\n--- PHASE 1b: OpenCode native ARM64 artifact ---\n\n'

OPENCODE_VERSION="1.18.30"
OPENCODE_URL="https://github.com/wallentx/opencode-termux/releases/download/v${OPENCODE_VERSION}-termux/opencode-android-arm64.tar.gz"
OPENCODE_SHA256="0856401391dca752313e32ef3a20f977700d9544d83605ab813c277190354c84"
OPENCODE_TARBALL="${LUBKO_OUTSIDE}/opencode-android-arm64.tar.gz"

printf '%s\n' '--- Download pinned OpenCode artifact ---'
python3 -c "
import urllib.request, sys
url = sys.argv[1]
dest = sys.argv[2]
print(f'Downloading {url} ...')
urllib.request.urlretrieve(url, dest)
print(f'Downloaded to {dest}')
" "$OPENCODE_URL" "$OPENCODE_TARBALL"

printf '%s\n' '--- Verify SHA256 ---'
python3 -c "
import hashlib, sys
path = sys.argv[1]
expected = sys.argv[2]
h = hashlib.sha256()
with open(path, 'rb') as f:
    for chunk in iter(lambda: f.read(65536), b''):
        h.update(chunk)
got = h.hexdigest()
if got != expected:
    print(f'FAIL: SHA256 mismatch: expected {expected}, got {got}')
    sys.exit(1)
print(f'SHA256 OK: {got}')
" "$OPENCODE_TARBALL" "$OPENCODE_SHA256"

printf '%s\n' '--- Extract and install to PATH ---'
python3 -c "
import tarfile, sys, os, stat, shutil
tarball = sys.argv[1]
dest = sys.argv[2]
os.makedirs(dest, exist_ok=True)
with tarfile.open(tarball, 'r:gz') as tf:
    members = tf.getmembers()
    if len(members) != 1 or members[0].name != 'opencode':
        names = [m.name for m in members]
        print(f'FAIL: expected single member named opencode, got {names}')
        sys.exit(1)
    if not members[0].isfile():
        print(f'FAIL: opencode member is not a regular file (type={members[0].type})')
        sys.exit(1)
    src = tf.extractfile(members[0])
    if src is None:
        print('FAIL: could not extract opencode member')
        sys.exit(1)
    dst = os.path.join(dest, 'opencode')
    with open(dst, 'wb') as out:
        shutil.copyfileobj(src, out)
    os.chmod(dst, os.stat(dst).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(f'Installed to {dst}')
" "$OPENCODE_TARBALL" "${BIN_HOME}"

printf '%s\n' '--- OpenCode version check ---'
OPENCODE_OUTPUT=$("${BIN_HOME}/opencode" version 2>&1) || {
  fail "opencode version exited non-zero"
  OPENCODE_OUTPUT=""
}
if printf '%s' "$OPENCODE_OUTPUT" | grep -qF "${OPENCODE_VERSION}"; then
  pass "opencode version reports ${OPENCODE_VERSION}"
else
  fail "opencode version did not report ${OPENCODE_VERSION}: ${OPENCODE_OUTPUT}"
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

# -- Canonical pytest budget check (hard 10 s) ------------------------------

printf '\n%s\n' '--- Canonical pytest budget check ---'
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
