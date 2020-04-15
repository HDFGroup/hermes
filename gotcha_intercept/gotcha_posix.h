#ifndef GOTCHA_POSIX_H_
#define GOTCHA_POSIX_H_

#include <unistd.h>

#include "gotcha/gotcha.h"
#include "gotcha/gotcha_types.h"

#define FUNC_COUNT(wraps) ((sizeof(wraps) / sizeof(wraps[0])))

gotcha_wrappee_handle_t orig_pwrite_handle;
typedef ssize_t (hermes_pwrite_fp)(int fd, const void *buf, size_t count, off_t offset);
ssize_t hermes_pwrite(int fd, const void *buf, size_t count, off_t offset);

gotcha_wrappee_handle_t orig_pread_handle;
typedef ssize_t (hermes_pread_fp)(int fd, void *buf, size_t count, off_t offset);
ssize_t hermes_pread(int fd, void *buf, size_t count, off_t offset);

gotcha_wrappee_handle_t orig_lseek_handle;
typedef off_t (hermes_lseek_fp)(int fd, off_t offset, int whence);
off_t hermes_lseek(int fd, off_t offset, int whence);

gotcha_wrappee_handle_t orig_write_handle;
typedef ssize_t (hermes_write_fp)(int fd, const void *buf, size_t count);
ssize_t hermes_write(int fd, const void *buf, size_t count);

gotcha_wrappee_handle_t orig_read_handle;
typedef ssize_t (hermes_read_fp)(int fd, void *buf, size_t count);
ssize_t hermes_read(int fd, void *buf, size_t count);

struct gotcha_binding_t wrap_posix [] = {
        { "pwrite", (void *)hermes_pwrite, &orig_pwrite_handle },
        { "pread", (void *)hermes_pread, &orig_pread_handle },
        { "lseek", (void *)hermes_lseek, &orig_lseek_handle },
        { "write", (void *)hermes_write, &orig_write_handle },
        { "read", (void *)hermes_read, &orig_read_handle },
};

void init_gotcha_posix ();

#endif  // GOTCHA_POSIX_H_
