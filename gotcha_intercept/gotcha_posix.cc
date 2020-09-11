extern "C" {
#include "gotcha_posix.h"
}

#include <stdio.h>
#include <unistd.h>

#include "gotcha/gotcha.h"
#include "gotcha/gotcha_types.h"

#define FUNC_COUNT(wraps) ((sizeof(wraps) / sizeof(wraps[0])))

gotcha_wrappee_handle_t orig_pwrite_handle;
typedef ssize_t (hermes_pwrite_fp)(int fd, const void *buf, size_t count,
                                   off_t offset);
ssize_t hermes_pwrite(int fd, const void *buf, size_t count, off_t offset);

gotcha_wrappee_handle_t orig_pread_handle;
typedef ssize_t (hermes_pread_fp)(int fd, void *buf, size_t count,
                                  off_t offset);
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

struct gotcha_binding_t wrap_posix[] = {
        { "pwrite", (void *)hermes_pwrite, &orig_pwrite_handle },
        { "pread", (void *)hermes_pread, &orig_pread_handle },
        { "lseek", (void *)hermes_lseek, &orig_lseek_handle },
        { "write", (void *)hermes_write, &orig_write_handle },
        { "read", (void *)hermes_read, &orig_read_handle },
};

void init_gotcha_posix() {
  gotcha_wrap(wrap_posix, FUNC_COUNT(wrap_posix), "hermes");
}

ssize_t hermes_pwrite(int fd, const void *buf, size_t count, off_t offset) {
  /** check if path matches prefix */
  bool path_match = 1;
  ssize_t ret = 0;

  printf("In wrapper hermes_pwrite\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function pwrite()\n");
    fflush(stdout);
  }

  hermes_pwrite_fp *orig_pwrite =
    (hermes_pwrite_fp *)gotcha_get_wrappee(orig_pwrite_handle);
  ret = orig_pwrite(fd, buf, count, offset);

  return ret;
}

ssize_t hermes_pread(int fd, void *buf, size_t count, off_t offset) {
  /** check if path matches prefix */
  bool path_match = 1;
  ssize_t ret = 0;

  printf("In wrapper hermes_pread\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function pread()\n");
    fflush(stdout);
  }

  hermes_pread_fp *orig_pread =
    (hermes_pread_fp *)gotcha_get_wrappee(orig_pread_handle);
  ret = orig_pread(fd, buf, count, offset);

  return ret;
}

off_t hermes_lseek(int fd, off_t offset, int whence) {
  /** check if path matches prefix */
  bool path_match = 1;
  off_t ret = 0;

  printf("In wrapper hermes_lseek\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function lseek()\n");
    fflush(stdout);
  }

  hermes_lseek_fp *orig_lseek =
    (hermes_lseek_fp *)gotcha_get_wrappee(orig_lseek_handle);
  ret = orig_lseek(fd, offset, whence);

  return ret;
}

ssize_t hermes_write(int fd, const void *buf, size_t count) {
  /** check if path matches prefix */
  bool path_match = 1;
  ssize_t ret = 0;

  printf("In wrapper hermes_write\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function write()\n");
    fflush(stdout);
  }
  hermes_write_fp *orig_write =
    (hermes_write_fp *)gotcha_get_wrappee(orig_write_handle);
  ret = orig_write(fd, buf, count);

  return ret;
}

ssize_t hermes_read(int fd, void *buf, size_t count) {
  /** check if path matches prefix */
  bool path_match = 1;
  ssize_t ret = 0;

  printf("In wrapper hermes_read\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function read()\n");
    fflush(stdout);
  }

  hermes_read_fp *orig_read =
    (hermes_read_fp *)gotcha_get_wrappee(orig_read_handle);
  ret = orig_read(fd, buf, count);

  return ret;
}
