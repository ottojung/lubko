/* Raw-syscall probe for the zero-space acceptance self-tests.
 *
 * Each subcommand performs exactly one path-mutating syscall and reports
 * "OP rc=<rc> errno=<errno>" on stdout, so acceptance.sh can assert the
 * tracer's verdict deterministically:
 *
 *   zsprobe linkat OLD NEW            linkat(AT_FDCWD, OLD, AT_FDCWD, NEW, 0)
 *   zsprobe symlinkat TARGET DIR NAME  symlinkat(TARGET, open(DIR), NAME)
 *   zsprobe fallocate PATH             open(PATH, O_WRONLY) + fallocate(fd,0,0,4096)
 *   zsprobe tee PATH                   open(PATH, O_WRONLY) + tee(pipe, fd, 4, 0)
 *
 * symlinkat takes DIR/NAME separately (not one absolute path) on purpose:
 * a dir-relative new path forces the tracer to read the new dirfd from the
 * correct register instead of reusing the target pointer.
 */

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
    if (argc < 2) {
        fprintf(stderr, "usage: zsprobe OP ...\n");
        return 2;
    }
    if (strcmp(argv[1], "linkat") == 0 && argc == 4) {
        long rc = (long)linkat(AT_FDCWD, argv[2], AT_FDCWD, argv[3], 0);
        report("linkat", rc);
        return 0;
    }
    if (strcmp(argv[1], "symlinkat") == 0 && argc == 5) {
        int dfd = open(argv[3], O_RDONLY | O_DIRECTORY);
        if (dfd < 0) {
            report("symlinkat-open", -1);
            return 0;
        }
        long rc = (long)symlinkat(argv[2], dfd, argv[4]);
        report("symlinkat", rc);
        close(dfd);
        return 0;
    }
    if (strcmp(argv[1], "fallocate") == 0 && argc == 3) {
        int fd = open(argv[2], O_WRONLY);
        if (fd < 0) {
            report("fallocate-open", -1);
            return 0;
        }
        long rc = (long)fallocate(fd, 0, 0, 4096);
        report("fallocate", rc);
        close(fd);
        return 0;
    }
    if (strcmp(argv[1], "tee") == 0 && argc == 3) {
        int fd = open(argv[2], O_WRONLY);
        if (fd < 0) {
            report("tee-open", -1);
            return 0;
        }
        int p[2];
        if (pipe(p) != 0) {
            report("tee-pipe", -1);
            close(fd);
            return 0;
        }
        const char *msg = "data";
        ssize_t w = write(p[1], msg, 4);
        (void)w;
        long rc = (long)tee(p[0], fd, 4, 0);
        report("tee", rc);
        close(p[0]);
        close(p[1]);
        close(fd);
        return 0;
    }
    fprintf(stderr, "zsprobe: unknown invocation\n");
    return 2;
}
