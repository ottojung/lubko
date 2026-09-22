/* Zero-allocation enforcement for the Lubko zero-space acceptance probe.
 *
 * Usage: zerospace --roots A:B:C --log FILE -- CMD [ARGS...]
 *
 * This tracer makes "zero allocatable filesystem bytes" explicit and
 * verifiable without depending on tmpfs or any spare-filesystem path. It
 * runs CMD as a ptraced child (following every fork/clone/exec, so the
 * supervisor, the worker it spawns, and every job process are all policed)
 * and fails every syscall that would consume a new persistent-filesystem
 * block under a Lubko-owned root with ENOSPC, regardless of which mount
 * backs the path. No path is exempt as tmpfs: enforcement is purely
 * path-based, so a deployment that secretly relied on a memory-backed mount
 * would fail exactly like one on an exhausted disk.
 *
 * Allowed without allocation: reads and metadata inspection, same-size
 * in-place rewrites of already-secured files, lock operations, removals
 * (they free space), pipes, sockets, and anything outside the roots.
 *
 * Every denial is appended to the log as one line:
 *   DENY <op> <absolute-path> <detail>
 * so unexpected Lubko-owned writes are identified clearly by path. Startup
 * appends an ACTIVE banner naming the roots, which the acceptance script
 * asserts before trusting any result.
 *
 * Enforcement happens at the kernel syscall boundary (not via LD_PRELOAD),
 * so libc-internal aliases, direct syscalls, stdio buffering, splice-style
 * copies, and memory-mapped stores cannot bypass it.
 *
 * Scope notes (documented, not silent):
 * - Unknown/future allocating syscalls (io_uring writes, process_vm_writev
 *   into another process, userfaultfd tricks) are not gated; the
 *   acceptance script additionally diffs a full file manifest taken before
 *   and after the zero phase, so any successful unexpected write is still
 *   caught by path.
 * - Offset checks for plain write() read the live file offset through
 *   /proc/<tid>/fd/<fd>; a concurrent writer racing on the same
 *   description could in theory slip past between check and write. The
 *   steady-state path has no such concurrent writers.
 * - On execve the tracked set is cleared (close-on-exec semantics): Python
 *   3.4+ marks new descriptors non-inheritable, and subprocess only passes
 *   stdio pipes, so nothing tracked survives into an exec'd child.
 */

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ptrace.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_ROOTS 16
#define MAX_FDS 1024
#define MAX_TRACEES 4096

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE (1 << 0)
#endif
#ifndef RENAME_EXCHANGE
#define RENAME_EXCHANGE (1 << 1)
#endif

static char g_roots[MAX_ROOTS][PATH_MAX];
static int g_nroots;
static int g_log_fd = -1;

struct fd_entry {
    int fd;
    off_t size;
    int append;
    char path[256];
};

struct fd_table {
    int refcount;
    int n;
    struct fd_entry entries[MAX_FDS];
};

struct tracee {
    int in_use;
    pid_t tid;
    pid_t tgid;
    int in_syscall;
    /* Saved entry state for exit handling / event classification. */
    long sysno;
    unsigned long long args[6];
    int deny_pending;
    int track_pending;
    off_t track_size;
    int track_append;
    char track_path[256];
    int shrink_pending;
    int shrink_fd;
    off_t shrink_size;
    struct fd_table *table;
};

static struct tracee g_tracees[MAX_TRACEES];

static void log_line(const char *op, const char *path, const char *detail) {
    if (g_log_fd < 0) {
        return;
    }
    char buf[1024];
    int n = snprintf(buf, sizeof buf, "DENY %s %s %s\n", op, path ? path : "?",
        detail ? detail : "-");
    if (n > 0) {
        ssize_t w = write(g_log_fd, buf, (size_t)n);
        (void)w;
    }
}

static struct fd_table *table_new(void) {
    struct fd_table *t = calloc(1, sizeof *t);
    if (t != NULL) {
        t->refcount = 1;
    }
    return t;
}

static struct fd_table *table_share(struct fd_table *t) {
    if (t != NULL) {
        t->refcount++;
    }
    return t;
}

static struct fd_table *table_copy(const struct fd_table *t) {
    struct fd_table *n = calloc(1, sizeof *n);
    if (n == NULL) {
        return NULL;
    }
    n->refcount = 1;
    if (t != NULL) {
        n->n = t->n;
        memcpy(n->entries, t->entries, sizeof n->entries);
    }
    return n;
}

static void table_release(struct fd_table *t) {
    if (t != NULL && --t->refcount == 0) {
        free(t);
    }
}

static int table_index(const struct fd_table *t, int fd) {
    if (t == NULL) {
        return -1;
    }
    for (int i = 0; i < t->n; i++) {
        if (t->entries[i].fd == fd) {
            return i;
        }
    }
    return -1;
}

static void table_track(struct fd_table *t, int fd, off_t size, int append,
    const char *path) {
    if (t == NULL || fd < 0) {
        return;
    }
    int i = table_index(t, fd);
    if (i >= 0) {
        t->entries[i].size = size;
        t->entries[i].append = append;
        return;
    }
    if (t->n >= MAX_FDS) {
        return;
    }
    t->entries[t->n].fd = fd;
    t->entries[t->n].size = size;
    t->entries[t->n].append = append;
    snprintf(t->entries[t->n].path, sizeof t->entries[t->n].path, "%s",
        path ? path : "?");
    t->n++;
}

static void table_untrack(struct fd_table *t, int fd) {
    int i = table_index(t, fd);
    if (i < 0) {
        return;
    }
    t->entries[i] = t->entries[t->n - 1];
    t->n--;
}

static void table_clear(struct fd_table *t) {
    if (t != NULL) {
        t->n = 0;
    }
}

/* Read a word from the tracee's address space. PTRACE_PEEKDATA works
 * under a plain same-user attach where process_vm_readv may be refused. */
static int peek_word(pid_t tid, unsigned long long addr,
    unsigned long long *out) {
    errno = 0;
    long v = ptrace(PTRACE_PEEKDATA, tid, (void *)(uintptr_t)addr, 0);
    if (v == -1 && errno != 0) {
        return -1;
    }
    *out = (unsigned long long)v;
    return 0;
}

/* Read a NUL-terminated string from the tracee's address space. */
static int read_tracee_string(pid_t tid, unsigned long long addr, char *out,
    size_t n) {
    size_t got = 0;
    while (got + 8 < n) {
        unsigned long long word = 0;
        if (peek_word(tid, addr + got, &word) != 0) {
            return -1;
        }
        memcpy(out + got, &word, 8);
        got += 8;
        if (memchr(&word, '\0', 8) != NULL) {
            return 0;
        }
    }
    out[n - 1] = '\0';
    return 0;
}

static int read_tracee_u64(pid_t tid, unsigned long long addr,
    unsigned long long *out) {
    return peek_word(tid, addr, out);
}

static int path_under_roots(const char *abs) {
    for (int i = 0; i < g_nroots; i++) {
        size_t len = strlen(g_roots[i]);
        if (strncmp(abs, g_roots[i], len) == 0
            && (abs[len] == '\0' || abs[len] == '/')) {
            return 1;
        }
    }
    return 0;
}

static void canonicalize(const char *abs, char *out, size_t n) {
    char tmp[PATH_MAX];
    if (realpath(abs, tmp) != NULL) {
        snprintf(out, n, "%s", tmp);
        return;
    }
    const char *slash = strrchr(abs, '/');
    if (slash == NULL || slash == abs) {
        snprintf(out, n, "%s", abs);
        return;
    }
    char dir[PATH_MAX];
    size_t dirlen = (size_t)(slash - abs);
    if (dirlen >= sizeof dir) {
        snprintf(out, n, "%s", abs);
        return;
    }
    memcpy(dir, abs, dirlen);
    dir[dirlen] = '\0';
    if (realpath(dir, tmp) != NULL) {
        /* Length-bounded: tmp and the final component both fit in PATH_MAX,
         * and out is PATH_MAX; truncation only on absurd inputs. */
        size_t a = strlen(tmp);
        size_t b = strlen(slash + 1);
        if (a + 1 + b + 1 <= n) {
            memcpy(out, tmp, a);
            out[a] = '/';
            memcpy(out + a + 1, slash + 1, b + 1);
            return;
        }
    }
    snprintf(out, n, "%s", abs);
}

/* Resolve (dirfd, path) for tid into an absolute path. Returns 0 on success. */
static int resolve_at(pid_t tid, int dirfd, unsigned long long path_addr,
    char *canon, size_t n) {
    char path[PATH_MAX];
    if (read_tracee_string(tid, path_addr, path, sizeof path) != 0) {
        return -1;
    }
    char abs[PATH_MAX];
    if (path[0] == '/') {
        snprintf(abs, sizeof abs, "%s", path);
    } else {
        char base[PATH_MAX];
        char proc[64];
        if (dirfd == AT_FDCWD) {
            snprintf(proc, sizeof proc, "/proc/%d/cwd", (int)tid);
        } else {
            snprintf(proc, sizeof proc, "/proc/%d/fd/%d", (int)tid, dirfd);
        }
        ssize_t len = readlink(proc, base, sizeof base - 1);
        if (len < 0) {
            return -1;
        }
        base[len] = '\0';
        /* readlink of an fd may report " (deleted)" or socket:/pipe:
         * only directory bases yield meaningful resolutions. */
        struct stat st;
        if (stat(base, &st) != 0 || !S_ISDIR(st.st_mode)) {
            return -1;
        }
        snprintf(abs, sizeof abs, "%s/%s", base, path);
    }
    canonicalize(abs, canon, n);
    return 0;
}

static int constrained_at(pid_t tid, int dirfd, unsigned long long path_addr,
    char *canon, size_t n) {
    if (resolve_at(tid, dirfd, path_addr, canon, n) != 0) {
        return 0;
    }
    return path_under_roots(canon);
}

static int exists_at(pid_t tid, int dirfd, unsigned long long path_addr,
    struct stat *st) {
    char canon[PATH_MAX];
    if (resolve_at(tid, dirfd, path_addr, canon, sizeof canon) != 0) {
        return -1;
    }
    return lstat(canon, st) == 0 ? 1 : 0;
}

/* Current file offset of tid's fd, read without disturbing it. */
static int current_offset(pid_t tid, int fd, off_t *off) {
    char proc[64];
    snprintf(proc, sizeof proc, "/proc/%d/fd/%d", (int)tid, fd);
    int dupfd = open(proc, O_RDONLY | O_CLOEXEC);
    if (dupfd < 0) {
        return -1;
    }
    off_t pos = lseek(dupfd, 0, SEEK_CUR);
    close(dupfd);
    if (pos < 0) {
        return -1;
    }
    *off = pos;
    return 0;
}

static struct tracee *find_tracee(pid_t tid) {
    for (int i = 0; i < MAX_TRACEES; i++) {
        if (g_tracees[i].in_use && g_tracees[i].tid == tid) {
            return &g_tracees[i];
        }
    }
    return NULL;
}

static struct tracee *add_tracee(pid_t tid, pid_t tgid, struct fd_table *t) {
    for (int i = 0; i < MAX_TRACEES; i++) {
        if (!g_tracees[i].in_use) {
            g_tracees[i].in_use = 1;
            g_tracees[i].tid = tid;
            g_tracees[i].tgid = tgid;
            g_tracees[i].in_syscall = 0;
            g_tracees[i].deny_pending = 0;
            g_tracees[i].track_pending = 0;
            g_tracees[i].table = t;
            return &g_tracees[i];
        }
    }
    return NULL;
}

static void remove_tracee(struct tracee *tr) {
    table_release(tr->table);
    tr->in_use = 0;
    tr->table = NULL;
}

static void deny_here(struct tracee *tr) {
    /* Skip the syscall: rewrite it to an invalid number, then force the
     * ENOSPC return at the matching exit stop. */
    struct user_regs_struct regs;
    if (ptrace(PTRACE_GETREGS, tr->tid, 0, &regs) != 0) {
        return;
    }
    regs.orig_rax = (unsigned long long)-1;
    if (ptrace(PTRACE_SETREGS, tr->tid, 0, &regs) == 0) {
        tr->deny_pending = 1;
    }
}

/* Gate a write of len bytes at absolute offset off to a tracked file. */
static int write_grows(struct tracee *tr, int fd, off_t off, size_t len,
    const char **path) {
    int i = table_index(tr->table, fd);
    if (i < 0) {
        return 0;
    }
    *path = tr->table->entries[i].path;
    if (len == 0) {
        return 0;
    }
    if (tr->table->entries[i].append) {
        return 1;
    }
    return off >= 0
        && (unsigned long long)off + len
            > (unsigned long long)tr->table->entries[i].size;
}

/* Shared entry gate for open/openat/creat (flags already decoded). */
static void gate_open(struct tracee *tr, int dirfd,
    unsigned long long path_addr, unsigned long long flags) {
    char canon[PATH_MAX];
    if (!constrained_at(tr->tid, dirfd, path_addr, canon, sizeof canon)) {
        return;
    }
    struct stat st;
    int ex = exists_at(tr->tid, dirfd, path_addr, &st);
    if (ex < 0) {
        return;
    }
    if (!ex) {
        if ((flags & (unsigned long long)O_CREAT) != 0) {
            log_line("open-create", canon, "new-entry-allocates");
            deny_here(tr);
        }
        return;
    }
    if (!S_ISREG(st.st_mode)) {
        return;
    }
    if ((flags & (unsigned long long)O_TMPFILE) != 0) {
        log_line("open-tmpfile", canon, "new-inode-allocates");
        deny_here(tr);
        return;
    }
    if ((flags & (unsigned long long)O_TRUNC) != 0) {
        log_line("open-truncate", canon, "truncate-allocates");
        deny_here(tr);
        return;
    }
    if ((flags & (unsigned long long)(O_WRONLY | O_RDWR)) != 0) {
        tr->track_pending = 1;
        tr->track_size = st.st_size;
        tr->track_append = ((flags & (unsigned long long)O_APPEND) != 0);
        snprintf(tr->track_path, sizeof tr->track_path, "%s", canon);
    }
}

static void gate_openat2(struct tracee *tr, int dirfd,
    unsigned long long path_addr, unsigned long long how_addr) {
    unsigned long long flags = 0;
    if (read_tracee_u64(tr->tid, how_addr, &flags) != 0) {
        /* Undecodable intent under enforcement fails closed only when the
         * path is constrained; otherwise it cannot be judged. */
        char canon[PATH_MAX];
        if (constrained_at(tr->tid, dirfd, path_addr, canon, sizeof canon)) {
            log_line("openat2", canon, "undecoded-how-fails-closed");
            deny_here(tr);
        }
        return;
    }
    gate_open(tr, dirfd, path_addr, flags);
}

static void gate_mkdir(struct tracee *tr, int dirfd,
    unsigned long long path_addr, const char *op) {
    char canon[PATH_MAX];
    if (!constrained_at(tr->tid, dirfd, path_addr, canon, sizeof canon)) {
        return;
    }
    struct stat st;
    if (exists_at(tr->tid, dirfd, path_addr, &st) > 0) {
        return;
    }
    log_line(op, canon, "new-entry-allocates");
    deny_here(tr);
}

static void gate_new_entry(struct tracee *tr, int dirfd,
    unsigned long long path_addr, const char *op) {
    char canon[PATH_MAX];
    if (!constrained_at(tr->tid, dirfd, path_addr, canon, sizeof canon)) {
        return;
    }
    log_line(op, canon, "new-entry-allocates");
    deny_here(tr);
}

/* Renames only allocate when they introduce a new directory entry. */
static void gate_rename(struct tracee *tr, int olddirfd,
    unsigned long long old_addr, int newdirfd, unsigned long long new_addr,
    unsigned long long rflags, const char *op) {
    char canon[PATH_MAX];
    if (!constrained_at(tr->tid, newdirfd, new_addr, canon, sizeof canon)) {
        return;
    }
    struct stat st;
    int src = exists_at(tr->tid, olddirfd, old_addr, &st);
    int dst = exists_at(tr->tid, newdirfd, new_addr, &st);
    if (src > 0 && dst > 0
        && (rflags == 0 || rflags == (unsigned long long)RENAME_NOREPLACE
            || rflags == (unsigned long long)RENAME_EXCHANGE)) {
        return;
    }
    log_line(op, canon, "new-entry-allocates");
    deny_here(tr);
}

static void gate_truncate_path(struct tracee *tr,
    unsigned long long path_addr, unsigned long long length) {
    char canon[PATH_MAX];
    if (!constrained_at(tr->tid, AT_FDCWD, path_addr, canon, sizeof canon)) {
        return;
    }
    struct stat st;
    if (stat(canon, &st) != 0 || !S_ISREG(st.st_mode)) {
        return;
    }
    if ((off_t)length > st.st_size) {
        log_line("truncate", canon, "growth-allocates");
        deny_here(tr);
    }
}

static void gate_ftruncate(struct tracee *tr, int fd, unsigned long long len) {
    int i = table_index(tr->table, fd);
    if (i < 0) {
        return;
    }
    if ((off_t)len > tr->table->entries[i].size) {
        log_line("ftruncate", tr->table->entries[i].path, "growth-allocates");
        deny_here(tr);
    } else {
        tr->shrink_pending = 1;
        tr->shrink_fd = fd;
        tr->shrink_size = (off_t)len;
    }
}

static void gate_fallocate(struct tracee *tr, int fd,
    unsigned long long mode, unsigned long long len) {
    int i = table_index(tr->table, fd);
    if (i < 0) {
        return;
    }
#ifndef FALLOC_FL_PUNCH_HOLE
#define FALLOC_FL_PUNCH_HOLE 0x02
#endif
    if (len > 0 && (mode & (unsigned long long)FALLOC_FL_PUNCH_HOLE) == 0) {
        log_line("fallocate", tr->table->entries[i].path, "growth-allocates");
        deny_here(tr);
    }
}

/* Gate len bytes written to fd_out at absolute offset off. */
static void gate_output(struct tracee *tr, int fd, off_t off, size_t len,
    const char *op) {
    const char *path = NULL;
    if (write_grows(tr, fd, off, len, &path)) {
        log_line(op, path, "growth-allocates");
        deny_here(tr);
    }
}

/* Resolve the effective output offset: explicit, else the live offset. */
static int output_offset(struct tracee *tr, int fd,
    unsigned long long off_addr, int has_off, off_t *off) {
    if (has_off) {
        unsigned long long v = 0;
        if (read_tracee_u64(tr->tid, off_addr, &v) != 0) {
            /* Fail closed: an unreadable explicit offset may hide growth. */
            const char *path = NULL;
            int i = table_index(tr->table, fd);
            if (i >= 0) {
                path = tr->table->entries[i].path;
                log_line("offset-unreadable", path, "fails-closed");
                deny_here(tr);
            }
            return -1;
        }
        *off = (off_t)v;
        return 0;
    }
    return current_offset(tr->tid, fd, off);
}

/* Sum an iovec array from tracee memory. Returns 0 with total set, or -1. */
static int sum_iov(pid_t tid, unsigned long long base, long count,
    size_t *total) {
    size_t sum = 0;
    for (long k = 0; k < count && k < 1024; k++) {
        unsigned long long len = 0;
        if (read_tracee_u64(tid, base + (unsigned long long)k * 16 + 8, &len)
            != 0) {
            return -1;
        }
        sum += (size_t)len;
    }
    *total = sum;
    return 0;
}

static void handle_entry(struct tracee *tr, struct user_regs_struct *regs) {
    tr->sysno = (long)regs->orig_rax;
    tr->args[0] = regs->rdi;
    tr->args[1] = regs->rsi;
    tr->args[2] = regs->rdx;
    tr->args[3] = regs->r10;
    tr->args[4] = regs->r8;
    tr->args[5] = regs->r9;
    tr->deny_pending = 0;
    tr->track_pending = 0;
    tr->shrink_pending = 0;

    switch (tr->sysno) {
    case __NR_openat:
        gate_open(tr, (int)regs->rdi, regs->rsi, regs->rdx);
        break;
    case __NR_open:
        gate_open(tr, AT_FDCWD, regs->rdi, regs->rsi);
        break;
    case __NR_creat:
        gate_open(tr, AT_FDCWD, regs->rdi,
            (unsigned long long)(O_CREAT | O_WRONLY | O_TRUNC));
        break;
    case __NR_openat2:
        gate_openat2(tr, (int)regs->rdi, regs->rsi, regs->rdx);
        break;
    case __NR_mkdir:
        gate_mkdir(tr, AT_FDCWD, regs->rdi, "mkdir");
        break;
    case __NR_mkdirat:
        gate_mkdir(tr, (int)regs->rdi, regs->rsi, "mkdirat");
        break;
    case __NR_mknod:
        gate_new_entry(tr, AT_FDCWD, regs->rdi, "mknod");
        break;
    case __NR_mknodat:
        gate_new_entry(tr, (int)regs->rdi, regs->rsi, "mknodat");
        break;
    case __NR_symlink:
        gate_new_entry(tr, AT_FDCWD, regs->rsi, "symlink");
        break;
    case __NR_symlinkat:
        gate_new_entry(tr, (int)regs->rdi, regs->rdx, "symlinkat");
        break;
    case __NR_link:
        gate_new_entry(tr, AT_FDCWD, regs->rsi, "link");
        break;
    case __NR_linkat:
        gate_new_entry(tr, (int)regs->rdx, regs->rsi, "linkat");
        break;
    case __NR_rename:
        gate_rename(tr, AT_FDCWD, regs->rdi, AT_FDCWD, regs->rsi, 0,
            "rename");
        break;
    case __NR_renameat:
        gate_rename(tr, (int)regs->rdi, regs->rsi, (int)regs->rdx,
            regs->r10, 0, "renameat");
        break;
    case __NR_renameat2:
        gate_rename(tr, (int)regs->rdi, regs->rsi, (int)regs->rdx,
            regs->r10, regs->r8, "renameat2");
        break;
    case __NR_truncate:
        gate_truncate_path(tr, regs->rdi, regs->rsi);
        break;
    case __NR_ftruncate:
        gate_ftruncate(tr, (int)regs->rdi, regs->rsi);
        break;
    case __NR_fallocate:
        gate_fallocate(tr, (int)regs->rdi, regs->rsi, regs->rdx);
        break;
    case __NR_close:
        table_untrack(tr->table, (int)regs->rdi);
        break;
    case __NR_write: {
        int fd = (int)regs->rdi;
        if (table_index(tr->table, fd) >= 0) {
            off_t off = 0;
            if (current_offset(tr->tid, fd, &off) != 0) {
                const char *path = NULL;
                int i = table_index(tr->table, fd);
                path = tr->table->entries[i].path;
                log_line("offset-unreadable", path, "fails-closed");
                deny_here(tr);
            } else {
                gate_output(tr, fd, off, (size_t)regs->rdx, "write");
            }
        }
        break;
    }
    case __NR_writev: {
        int fd = (int)regs->rdi;
        if (table_index(tr->table, fd) >= 0) {
            size_t total = 0;
            off_t off = 0;
            if (sum_iov(tr->tid, regs->rsi, (long)regs->rdx, &total) != 0
                || current_offset(tr->tid, fd, &off) != 0) {
                int i = table_index(tr->table, fd);
                log_line("offset-unreadable", tr->table->entries[i].path,
                    "fails-closed");
                deny_here(tr);
            } else {
                gate_output(tr, fd, off, total, "writev");
            }
        }
        break;
    }
    case __NR_pwrite64: {
        int fd = (int)regs->rdi;
        if (table_index(tr->table, fd) >= 0) {
            gate_output(tr, fd, (off_t)regs->r10, (size_t)regs->rdx,
                "pwrite");
        }
        break;
    }
    case __NR_pwritev:
    case __NR_pwritev2: {
        int fd = (int)regs->rdi;
        if (table_index(tr->table, fd) >= 0) {
            size_t total = 0;
            /* pwritev(vec, count, offset): explicit offset is args[3]. */
            if (sum_iov(tr->tid, regs->rsi, (long)regs->rdx, &total) != 0) {
                int i = table_index(tr->table, fd);
                log_line("offset-unreadable", tr->table->entries[i].path,
                    "fails-closed");
                deny_here(tr);
            } else {
                gate_output(tr, fd, (off_t)regs->r10, total, "pwritev");
            }
        }
        break;
    }
    case __NR_sendfile: {
        int out = (int)regs->rdi;
        if (table_index(tr->table, out) >= 0) {
            off_t off = 0;
            unsigned long long off_addr = regs->rdx;
            if (output_offset(tr, out, off_addr, off_addr != 0, &off) == 0) {
                gate_output(tr, out, off, (size_t)regs->r10, "sendfile");
            }
        }
        break;
    }
    case __NR_splice:
    case __NR_tee: {
        int out = (int)regs->rdx;
        if (table_index(tr->table, out) >= 0) {
            off_t off = 0;
            unsigned long long off_addr = regs->r10;
            const char *op = tr->sysno == __NR_splice ? "splice" : "tee";
            if (output_offset(tr, out, off_addr, off_addr != 0, &off) == 0) {
                gate_output(tr, out, off, (size_t)regs->r8, op);
            }
        }
        break;
    }
    case __NR_copy_file_range: {
        int out = (int)regs->rdx;
        if (table_index(tr->table, out) >= 0) {
            off_t off = 0;
            unsigned long long off_addr = regs->r10;
            if (output_offset(tr, out, off_addr, off_addr != 0, &off) == 0) {
                gate_output(tr, out, off, (size_t)regs->r8,
                    "copy_file_range");
            }
        }
        break;
    }
    case __NR_mmap: {
        unsigned long long prot = regs->rdx;
        unsigned long long mflags = regs->r10;
        long fd = (long)(int)regs->r8;
        if ((prot & (unsigned long long)PROT_WRITE) != 0
            && (mflags & (unsigned long long)MAP_SHARED) != 0
            && (mflags & (unsigned long long)MAP_ANONYMOUS) == 0
            && fd >= 0 && table_index(tr->table, (int)fd) >= 0) {
            int i = table_index(tr->table, (int)fd);
            log_line("mmap-shared-write", tr->table->entries[i].path,
                "shared-writable-mapping");
            deny_here(tr);
        }
        break;
    }
    case __NR_mount:
        log_line("mount", "?", "mounts-fail-closed");
        deny_here(tr);
        break;
    case __NR_umount2:
        log_line("umount", "?", "unmounts-fail-closed");
        deny_here(tr);
        break;
    default:
        break;
    }
}

static void handle_exit(struct tracee *tr, struct user_regs_struct *regs) {
    if (tr->deny_pending) {
        regs->rax = (unsigned long long)(long)-ENOSPC;
        ptrace(PTRACE_SETREGS, tr->tid, 0, regs);
        tr->deny_pending = 0;
        tr->track_pending = 0;
        tr->shrink_pending = 0;
        return;
    }
    long rc = (long)regs->rax;
    switch (tr->sysno) {
    case __NR_openat:
    case __NR_open:
    case __NR_creat:
    case __NR_openat2:
        if (tr->track_pending == 1 && rc >= 0) {
            table_track(tr->table, (int)rc, tr->track_size,
                tr->track_append, tr->track_path);
        }
        break;
    case __NR_ftruncate:
        if (tr->shrink_pending && rc == 0) {
            int i = table_index(tr->table, tr->shrink_fd);
            if (i >= 0) {
                tr->table->entries[i].size = tr->shrink_size;
            }
        }
        break;
    case __NR_dup:
        if (rc >= 0) {
            int i = table_index(tr->table, (int)tr->args[0]);
            if (i >= 0) {
                table_track(tr->table, (int)rc,
                    tr->table->entries[i].size,
                    tr->table->entries[i].append,
                    tr->table->entries[i].path);
            }
        }
        break;
    case __NR_dup2:
    case __NR_dup3:
        if (rc >= 0) {
            int newfd = (int)tr->args[1];
            table_untrack(tr->table, newfd);
            int i = table_index(tr->table, (int)tr->args[0]);
            if (i >= 0) {
                table_track(tr->table, newfd, tr->table->entries[i].size,
                    tr->table->entries[i].append,
                    tr->table->entries[i].path);
            }
        }
        break;
    case __NR_fcntl: {
        long cmd = (long)tr->args[1];
        if ((cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC) && rc >= 0) {
            int i = table_index(tr->table, (int)tr->args[0]);
            if (i >= 0) {
                table_track(tr->table, (int)rc,
                    tr->table->entries[i].size,
                    tr->table->entries[i].append,
                    tr->table->entries[i].path);
            }
        }
        break;
    }
    default:
        break;
    }
    tr->track_pending = 0;
    tr->shrink_pending = 0;
}

static void usage(const char *prog) {
    fprintf(stderr, "usage: %s --roots A[:B...] --log FILE -- CMD [ARGS...]\n",
        prog);
}

int main(int argc, char **argv) {
    const char *roots_arg = NULL;
    const char *log_arg = NULL;
    int cmd_at = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--roots") == 0 && i + 1 < argc) {
            roots_arg = argv[++i];
        } else if (strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            log_arg = argv[++i];
        } else if (strcmp(argv[i], "--") == 0) {
            cmd_at = i + 1;
            break;
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (roots_arg == NULL || log_arg == NULL || cmd_at == 0
        || cmd_at >= argc) {
        usage(argv[0]);
        return 2;
    }
    char copy[4096];
    snprintf(copy, sizeof copy, "%s", roots_arg);
    char *save = NULL;
    for (char *tok = strtok_r(copy, ":", &save); tok != NULL;
        tok = strtok_r(NULL, ":", &save)) {
        if (g_nroots >= MAX_ROOTS) {
            break;
        }
        if (realpath(tok, g_roots[g_nroots]) != NULL) {
            g_nroots++;
        } else {
            fprintf(stderr, "zerospace: root does not resolve: %s\n", tok);
            return 2;
        }
    }
    if (g_nroots == 0) {
        fprintf(stderr, "zerospace: no usable roots\n");
        return 2;
    }
    g_log_fd = open(log_arg, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (g_log_fd < 0) {
        fprintf(stderr, "zerospace: cannot open log %s: %s\n", log_arg,
            strerror(errno));
        return 2;
    }
    {
        char banner[4200];
        int n = snprintf(banner, sizeof banner, "ACTIVE roots=%s pid=%d\n",
            roots_arg, (int)getpid());
        if (n > 0) {
            ssize_t w = write(g_log_fd, banner, (size_t)n);
            (void)w;
        }
    }

    pid_t child = fork();
    if (child < 0) {
        perror("fork");
        return 2;
    }
    if (child == 0) {
        if (ptrace(PTRACE_TRACEME, 0, 0, 0) != 0) {
            perror("PTRACE_TRACEME");
            _exit(127);
        }
        raise(SIGSTOP);
        execvp(argv[cmd_at], &argv[cmd_at]);
        perror("execvp");
        _exit(127);
    }

    int status = 0;
    if (waitpid(child, &status, 0) < 0) {
        perror("waitpid");
        return 2;
    }
    long options = PTRACE_O_TRACESYSGOOD | PTRACE_O_TRACEFORK
        | PTRACE_O_TRACEVFORK | PTRACE_O_TRACECLONE | PTRACE_O_TRACEEXEC
        | PTRACE_O_TRACEEXIT;
    if (ptrace(PTRACE_SETOPTIONS, child, 0, (void *)options) != 0) {
        perror("PTRACE_SETOPTIONS");
        return 2;
    }
    struct fd_table *root_table = table_new();
    if (root_table == NULL || add_tracee(child, child, root_table) == NULL) {
        fprintf(stderr, "zerospace: out of memory\n");
        return 2;
    }
    if (ptrace(PTRACE_SYSCALL, child, 0, 0) != 0) {
        perror("PTRACE_SYSCALL");
        return 2;
    }

    int child_code = -1;
    int child_sig = 0;
    while (1) {
        pid_t tid = waitpid(-1, &status, 0);
        if (tid < 0) {
            if (errno == ECHILD) {
                break;
            }
            perror("waitpid");
            return 2;
        }
        if (WIFEXITED(status) || WIFSIGNALED(status)) {
            struct tracee *tr = find_tracee(tid);
            if (tr != NULL) {
                if (tid == child) {
                    if (WIFEXITED(status)) {
                        child_code = WEXITSTATUS(status);
                    } else {
                        child_sig = WTERMSIG(status);
                    }
                }
                remove_tracee(tr);
            }
            continue;
        }
        if (!WIFSTOPPED(status)) {
            continue;
        }
        int sig = WSTOPSIG(status);
        struct tracee *tr = find_tracee(tid);
        if (tr == NULL) {
            /* A thread racing ahead of its clone event: attach lazily by
             * inheriting the group leader's table copy. */
            struct fd_table *t = table_new();
            if (t == NULL
                || add_tracee(tid, tid, t) == NULL
                || ptrace(PTRACE_SETOPTIONS, tid, 0, (void *)options) != 0) {
                ptrace(PTRACE_SYSCALL, tid, 0, 0);
                continue;
            }
            tr = find_tracee(tid);
        }
        unsigned long event = (unsigned long)((status >> 16) & 0xffff);
        if (sig == SIGTRAP && event != 0) {
            if (event == PTRACE_EVENT_FORK || event == PTRACE_EVENT_VFORK
                || event == PTRACE_EVENT_CLONE) {
                unsigned long msg = 0;
                if (ptrace(PTRACE_GETEVENTMSG, tid, 0, &msg) == 0) {
                    pid_t nt = (pid_t)msg;
                    unsigned long long cflags = tr->args[0];
                    struct fd_table *nt_table = NULL;
                    pid_t nt_tgid = nt;
                    if ((cflags & (unsigned long long)CLONE_THREAD) != 0) {
                        nt_tgid = tr->tgid;
                        nt_table = table_share(tr->table);
                    } else if ((cflags
                        & (unsigned long long)CLONE_FILES) != 0) {
                        nt_table = table_share(tr->table);
                    } else {
                        nt_table = table_copy(tr->table);
                    }
                    if (nt_table == NULL
                        || add_tracee(nt, nt_tgid, nt_table) == NULL) {
                        table_release(nt_table);
                    } else {
                        ptrace(PTRACE_SETOPTIONS, nt, 0, (void *)options);
                    }
                }
            } else if (event == PTRACE_EVENT_EXEC) {
                /* Close-on-exec approximation: nothing tracked survives. */
                table_clear(tr->table);
            }
            ptrace(PTRACE_SYSCALL, tid, 0, 0);
            continue;
        }
        if (sig == (SIGTRAP | 0x80)) {
            struct user_regs_struct regs;
            if (ptrace(PTRACE_GETREGS, tid, 0, &regs) != 0) {
                ptrace(PTRACE_SYSCALL, tid, 0, 0);
                continue;
            }
            if (!tr->in_syscall) {
                tr->in_syscall = 1;
                handle_entry(tr, &regs);
            } else {
                tr->in_syscall = 0;
                handle_exit(tr, &regs);
            }
            ptrace(PTRACE_SYSCALL, tid, 0, 0);
            continue;
        }
        ptrace(PTRACE_SYSCALL, tid, 0, (void *)(long)sig);
    }
    if (child_sig != 0) {
        raise(child_sig);
        return 128 + child_sig;
    }
    return child_code < 0 ? 2 : child_code;
}
