"""Zero-space tracer dispatch invariants for path-mutating syscalls.

The ptrace tracer in ``acceptance/zero_space/zerospace.c`` must deny every
allocating syscall under a constrained root with ``ENOSPC`` while letting the
same operation succeed outside the roots. Each test pairs a denied case
(inside) with a permitted case (outside) so an argument-index mixup that
polices the wrong path or length fails deterministically.
"""

from __future__ import annotations

import errno
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

_REPO = Path(__file__).resolve().parent.parent
_ZEROSPACE_SRC = _REPO / "acceptance" / "zero_space" / "zerospace.c"

_PROBE_SRC = r"""
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
static void report(const char *op, long rc) {
    int e = (rc == -1) ? errno : 0;
    printf("%s rc=%ld errno=%d\n", op, rc, e);
    fflush(stdout);
}
int main(int argc, char **argv) {
    if (argc < 2) return 2;
    if (strcmp(argv[1], "linkat") == 0 && argc == 4) {
        long rc = (long)linkat(AT_FDCWD, argv[2], AT_FDCWD, argv[3], 0);
        report("linkat", rc);
        return 0;
    }
    if (strcmp(argv[1], "symlinkat") == 0 && argc == 5) {
        /* symlinkat(target, dirfd, name): dir-relative on purpose, so the
         * tracer must read the dirfd from args[1], not the target pointer. */
        int dfd = open(argv[3], O_RDONLY | O_DIRECTORY);
        if (dfd < 0) { report("symlinkat-open", -1); return 0; }
        long rc = (long)symlinkat(argv[2], dfd, argv[4]);
        report("symlinkat", rc);
        close(dfd);
        return 0;
    }
    if (strcmp(argv[1], "fallocate") == 0 && argc == 3) {
        int fd = open(argv[2], O_WRONLY);
        if (fd < 0) { report("fallocate-open", -1); return 0; }
        long rc = (long)fallocate(fd, 0, 0, 4096);
        report("fallocate", rc);
        close(fd);
        return 0;
    }
    if (strcmp(argv[1], "tee") == 0 && argc == 3) {
        int fd = open(argv[2], O_WRONLY);
        if (fd < 0) { report("tee-open", -1); return 0; }
        int p[2];
        if (pipe(p) != 0) { report("tee-pipe", -1); close(fd); return 0; }
        const char *msg = "data";
        ssize_t w = write(p[1], msg, 4);
        (void)w;
        long rc = (long)tee(p[0], fd, 4, 0);
        report("tee", rc);
        close(p[0]); close(p[1]); close(fd);
        return 0;
    }
    fprintf(stderr, "unknown probe\n");
    return 2;
}
"""


def _compiler() -> str:
    """Locate a C compiler for the tracer and probe builds.

    Returns:
        The compiler binary path.
    """
    for candidate in (os.environ.get("ZERO_SPACE_CC", ""), "cc", "gcc"):
        if candidate and shutil.which(candidate) is not None:
            return candidate
    pytest.fail("no C compiler available for zerospace tracer self-test")


def _kernel_include_flags() -> list[str]:
    """Locate Guix kernel headers missing from the default include path.

    Returns:
        ``-I`` flags for every candidate headers directory that exists.
    """
    flags: list[str] = []
    try:
        store = Path("/gnu/store")
        candidates = sorted(store.glob("*linux-libre-headers-*/include"))
    except OSError:
        return flags
    for candidate in candidates:
        if (candidate / "linux" / "errno.h").exists():
            flags.append(f"-I{candidate}")
            break
    return flags


@pytest.fixture(scope="module")
def binaries() -> Iterator[tuple[Path, Path]]:
    """Build the tracer and the raw-syscall probe once per session.

    Yields:
        Paths to the ``zerospace`` tracer and the ``zsprobe`` helper.
    """
    # Pytest's default basetemp lives on a noexec mount here, so binaries
    # are built in an exec-capable scratch area instead of tmp_path_factory.
    parent = Path.home()
    work = Path(tempfile.mkdtemp(prefix="zerospace-bins-", dir=str(parent)))
    try:
        tracer = work / "zerospace"
        probe_src = work / "probe.c"
        probe = work / "zsprobe"
        probe_src.write_text(_PROBE_SRC)
        cc = _compiler()
        includes = _kernel_include_flags()
        for src, out in ((_ZEROSPACE_SRC, tracer), (probe_src, probe)):
            completed = subprocess.run(
                (cc, "-O0", *includes, "-o", str(out), str(src)),
                capture_output=True,
                text=True,
                check=False,
            )
            assert completed.returncode == 0, f"compile {src.name} failed: {completed.stderr}"
        yield tracer, probe
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _run_traced(
    tracer: Path, roots: str, log: Path, probe: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    """Run one probe operation under zero-allocation enforcement.

    Returns:
        The completed probe process result.
    """
    return subprocess.run(
        (str(tracer), "--roots", roots, "--log", str(log), "--", str(probe), *args),
        capture_output=True,
        text=True,
        check=False,
    )


def _errno_of(output: str, op: str) -> int:
    """Extract the reported errno for one probe operation from its stdout.

    Returns:
        The numeric errno the probe observed.
    """
    for line in output.splitlines():
        if line.startswith(op + " "):
            for field in line.split():
                if field.startswith("errno="):
                    return int(field.split("=", 1)[1])
    msg = f"probe reported no {op} line in: {output!r}"
    pytest.fail(msg)


def test_linkat_new_path_denied_inside_permitted_outside(
    binaries: tuple[Path, Path], tmp_path: Path
) -> None:
    """Deny new hard-link entries created with linkat inside the roots."""
    tracer, probe = binaries
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    src = outside / "src"
    src.write_bytes(b"x")
    denied = root / "linked"
    log = tmp_path / "linkat.log"
    result = _run_traced(tracer, str(root), log, probe, "linkat", str(src), str(denied))
    assert result.returncode == 0
    assert _errno_of(result.stdout, "linkat") == errno.ENOSPC
    assert not denied.exists()
    assert "linkat" in log.read_text()

    ok_dst = outside / "linked-ok"
    ok_log = tmp_path / "linkat-ok.log"
    ok_src = outside / "src2"
    ok_src.write_bytes(b"x")
    ok = _run_traced(tracer, str(root), ok_log, probe, "linkat", str(ok_src), str(ok_dst))
    assert ok.returncode == 0
    assert _errno_of(ok.stdout, "linkat") == 0
    assert ok_dst.exists()


def test_symlinkat_new_path_denied_inside_permitted_outside(
    binaries: tuple[Path, Path], tmp_path: Path
) -> None:
    """Deny new link paths created with symlinkat inside the roots."""
    tracer, probe = binaries
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    denied = root / "alias"
    log = tmp_path / "symlinkat.log"
    result = _run_traced(tracer, str(root), log, probe, "symlinkat", "target", str(root), "alias")
    assert result.returncode == 0
    assert _errno_of(result.stdout, "symlinkat") == errno.ENOSPC
    assert not denied.exists()
    assert not denied.is_symlink()

    ok_dst = outside / "alias-ok"
    ok_log = tmp_path / "symlinkat-ok.log"
    ok = _run_traced(
        tracer,
        str(root),
        ok_log,
        probe,
        "symlinkat",
        "target",
        str(outside),
        "alias-ok",
    )
    assert ok.returncode == 0
    assert _errno_of(ok.stdout, "symlinkat") == 0
    assert ok_dst.is_symlink()


def test_fallocate_growth_denied_inside_permitted_outside(
    binaries: tuple[Path, Path], tmp_path: Path
) -> None:
    """Deny fallocate growth inside the roots; allow it outside them."""
    tracer, probe = binaries
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = root / "file"
    victim.write_bytes(b"")
    log = tmp_path / "fallocate.log"
    result = _run_traced(tracer, str(root), log, probe, "fallocate", str(victim))
    assert result.returncode == 0
    assert _errno_of(result.stdout, "fallocate") == errno.ENOSPC
    assert victim.stat().st_size == 0

    control = outside / "file"
    control.write_bytes(b"")
    ok_log = tmp_path / "fallocate-ok.log"
    ok = _run_traced(tracer, str(root), ok_log, probe, "fallocate", str(control))
    assert ok.returncode == 0
    assert _errno_of(ok.stdout, "fallocate") == 0
    assert control.stat().st_size == 4096


def test_tee_growth_denied_inside_permitted_outside(
    binaries: tuple[Path, Path], tmp_path: Path
) -> None:
    """Deny tee growth inside the roots; pass the call through outside."""
    tracer, probe = binaries
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = root / "sink"
    victim.write_bytes(b"")
    log = tmp_path / "tee.log"
    result = _run_traced(tracer, str(root), log, probe, "tee", str(victim))
    assert result.returncode == 0
    assert _errno_of(result.stdout, "tee") == errno.ENOSPC
    assert victim.stat().st_size == 0

    # tee to a regular file is rejected by the kernel itself (EINVAL: both
    # ends must be pipes), so "behaves normally outside" means the tracer
    # passes the call through untouched: same errno as the untraced probe.
    control = outside / "sink"
    control.write_bytes(b"")
    native = subprocess.run(
        (str(probe), "tee", str(control)),
        capture_output=True,
        text=True,
        check=False,
    )
    assert native.returncode == 0
    native_errno = _errno_of(native.stdout, "tee")
    assert native_errno != errno.ENOSPC
    ok_log = tmp_path / "tee-ok.log"
    ok = _run_traced(tracer, str(root), ok_log, probe, "tee", str(control))
    assert ok.returncode == 0
    assert _errno_of(ok.stdout, "tee") == native_errno
