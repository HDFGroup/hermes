/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_ADAPTER_POSIX_H
#define HERMES_ADAPTER_POSIX_H
#include <string>
#include <iostream>
#include "hermes_shm/util/logging.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include "io_client/real_api.h"

#ifndef O_TMPFILE
#define O_TMPFILE 0
#endif

#ifndef _STAT_VER
#define _STAT_VER 0
#endif

#define REQUIRE_API(api_name) \
  if (api_name == nullptr) { \
    HELOG(kFatal, "HERMES Adapter failed to map symbol: {}", #api_name); \
  }

extern "C" {
typedef int (*open_t)(const char * path, int flags,  ...);
typedef int (*open64_t)(const char * path, int flags,  ...);
typedef int (*__open_2_t)(const char * path, int oflag);
typedef int (*creat_t)(const char * path, mode_t mode);
typedef int (*creat64_t)(const char * path, mode_t mode);
typedef ssize_t (*read_t)(int fd, void * buf, size_t count);
typedef ssize_t (*write_t)(int fd, const void * buf, size_t count);
typedef ssize_t (*pread_t)(int fd, void * buf, size_t count, off_t offset);
typedef ssize_t (*pwrite_t)(int fd, const void * buf, size_t count, off_t offset);
typedef ssize_t (*pread64_t)(int fd, void * buf, size_t count, off64_t offset);
typedef ssize_t (*pwrite64_t)(int fd, const void * buf, size_t count, off64_t offset);
typedef off_t (*lseek_t)(int fd, off_t offset, int whence);
typedef off64_t (*lseek64_t)(int fd, off64_t offset, int whence);
typedef int (*__fxstat_t)(int __ver, int __filedesc, struct stat * __stat_buf);
typedef int (*fstat_t)(int __filedesc, struct stat * __stat_buf);
typedef int (*fsync_t)(int fd);
typedef int (*close_t)(int fd);

typedef int (*fchdir_t)(int fd);
typedef int (*fchmod_t)(int fd, mode_t mode);
typedef int (*fchmod_t)(int fd, mode_t mode);
typedef int (*fchown_t)(int fd, uid_t owner, gid_t group);
typedef int (*fchownat_t)(int dirfd, const char *pathname,
                          uid_t owner, gid_t group, int flags);
typedef int (*close_range_t)(unsigned int first, unsigned int last,
                             unsigned int flags);
typedef ssize_t (*copy_file_range_t)(int fd_in, off64_t *off_in,
                                     int fd_out, off64_t *off_out,
                                     size_t len, unsigned int flags);

// TODO(llogan): fadvise
typedef int (*posix_fadvise_t)(int fd, off_t offset,
                               off_t len, int advice);
typedef int (*flock_t)(int fd, int operation);
typedef int (*remove_t)(const char *pathname);
typedef int (*unlink_t)(const char *pathname);
}

namespace hermes::adapter::fs {

/** Used for compatability with older kernel versions */
static int fxstat_to_fstat(int fd, struct stat * stbuf);

/** Pointers to the real posix API */
class PosixApi : public RealApi {
 public:
  /** open */
  open_t open = nullptr;
  /** open64 */
  open64_t open64 = nullptr;
  /** __open_2 */
  __open_2_t __open_2 = nullptr;
  /** creat */
  creat_t creat = nullptr;
  /** creat64 */
  creat64_t creat64 = nullptr;
  /** read */
  read_t read = nullptr;
  /** write */
  write_t write = nullptr;
  /** pread */
  pread_t pread = nullptr;
  /** pwrite */
  pwrite_t pwrite = nullptr;
  /** pread64 */
  pread64_t pread64 = nullptr;
  /** pwrite64 */
  pwrite64_t pwrite64 = nullptr;
  /** lseek */
  lseek_t lseek = nullptr;
  /** lseek64 */
  lseek64_t lseek64 = nullptr;
  /** __fxstat */
  __fxstat_t __fxstat = nullptr;
  /** fstat */
  fstat_t fstat = nullptr;
  /** fsync */
  fsync_t fsync = nullptr;
  /** close */
  close_t close = nullptr;
  /** flock */
  flock_t flock = nullptr;
  /** remove */
  remove_t remove = nullptr;
  /** unlink */
  unlink_t unlink = nullptr;

  PosixApi() : RealApi("open", "posix_intercepted") {
    if (is_intercepted_) {
      open = (open_t)dlsym(real_lib_next_, "open");
    } else {
      open = (open_t)dlsym(real_lib_default_, "open");
    }
    REQUIRE_API(open)
    if (is_intercepted_) {
      open64 = (open64_t)dlsym(real_lib_next_, "open64");
    } else {
      open64 = (open64_t)dlsym(real_lib_default_, "open64");
    }
    REQUIRE_API(open64)
    if (is_intercepted_) {
      __open_2 = (__open_2_t)dlsym(real_lib_next_, "__open_2");
    } else {
      __open_2 = (__open_2_t)dlsym(real_lib_default_, "__open_2");
    }
    REQUIRE_API(__open_2)
    if (is_intercepted_) {
      creat = (creat_t)dlsym(real_lib_next_, "creat");
    } else {
      creat = (creat_t)dlsym(real_lib_default_, "creat");
    }
    REQUIRE_API(creat)
    if (is_intercepted_) {
      creat64 = (creat64_t)dlsym(real_lib_next_, "creat64");
    } else {
      creat64 = (creat64_t)dlsym(real_lib_default_, "creat64");
    }
    REQUIRE_API(creat64)
    if (is_intercepted_) {
      read = (read_t)dlsym(real_lib_next_, "read");
    } else {
      read = (read_t)dlsym(real_lib_default_, "read");
    }
    REQUIRE_API(read)
    if (is_intercepted_) {
      write = (write_t)dlsym(real_lib_next_, "write");
    } else {
      write = (write_t)dlsym(real_lib_default_, "write");
    }
    REQUIRE_API(write)
    if (is_intercepted_) {
      pread = (pread_t)dlsym(real_lib_next_, "pread");
    } else {
      pread = (pread_t)dlsym(real_lib_default_, "pread");
    }
    REQUIRE_API(pread)
    if (is_intercepted_) {
      pwrite = (pwrite_t)dlsym(real_lib_next_, "pwrite");
    } else {
      pwrite = (pwrite_t)dlsym(real_lib_default_, "pwrite");
    }
    REQUIRE_API(pwrite)
    if (is_intercepted_) {
      pread64 = (pread64_t)dlsym(real_lib_next_, "pread64");
    } else {
      pread64 = (pread64_t)dlsym(real_lib_default_, "pread64");
    }
    REQUIRE_API(pread64)
    if (is_intercepted_) {
      pwrite64 = (pwrite64_t)dlsym(real_lib_next_, "pwrite64");
    } else {
      pwrite64 = (pwrite64_t)dlsym(real_lib_default_, "pwrite64");
    }
    REQUIRE_API(pwrite64)
    if (is_intercepted_) {
      lseek = (lseek_t)dlsym(real_lib_next_, "lseek");
    } else {
      lseek = (lseek_t)dlsym(real_lib_default_, "lseek");
    }
    REQUIRE_API(lseek)
    if (is_intercepted_) {
      lseek64 = (lseek64_t)dlsym(real_lib_next_, "lseek64");
    } else {
      lseek64 = (lseek64_t)dlsym(real_lib_default_, "lseek64");
    }
    REQUIRE_API(lseek64)
    if (is_intercepted_) {
      __fxstat = (__fxstat_t)dlsym(real_lib_next_, "__fxstat");
    } else {
      __fxstat = (__fxstat_t)dlsym(real_lib_default_, "__fxstat");
    }
    // REQUIRE_API(__fxstat)
    if (is_intercepted_) {
      fstat = (fstat_t)dlsym(real_lib_next_, "fstat");
    } else {
      fstat = (fstat_t)dlsym(real_lib_default_, "fstat");
    }
    if (fstat == nullptr) {
      fstat = fxstat_to_fstat;
    }
    // REQUIRE_API(fstat)
    if (is_intercepted_) {
      fsync = (fsync_t)dlsym(real_lib_next_, "fsync");
    } else {
      fsync = (fsync_t)dlsym(real_lib_default_, "fsync");
    }
    REQUIRE_API(fsync)
    if (is_intercepted_) {
      close = (close_t)dlsym(real_lib_next_, "close");
    } else {
      close = (close_t)dlsym(real_lib_default_, "close");
    }
    REQUIRE_API(close)
    if (is_intercepted_) {
      flock = (flock_t)dlsym(real_lib_next_, "flock");
    } else {
      flock = (flock_t)dlsym(real_lib_default_, "flock");
    }
    REQUIRE_API(flock)
    if (is_intercepted_) {
      remove = (remove_t)dlsym(real_lib_next_, "remove");
    } else {
      remove = (remove_t)dlsym(real_lib_default_, "remove");
    }
    REQUIRE_API(remove)
    if (is_intercepted_) {
      unlink = (unlink_t)dlsym(real_lib_next_, "unlink");
    } else {
      unlink = (unlink_t)dlsym(real_lib_default_, "unlink");
    }
    REQUIRE_API(unlink)
  }
};

}  // namespace hermes::adapter::fs

// Singleton macros
#include "hermes_shm/util/singleton.h"

#define HERMES_POSIX_API \
  hshm::Singleton<hermes::adapter::fs::PosixApi>::GetInstance()
#define HERMES_POSIX_API_T hermes::adapter::fs::PosixApi*

#undef REQUIRE_API

namespace hermes::adapter::fs {
/** Used for compatability with older kernel versions */
static int fxstat_to_fstat(int fd, struct stat *stbuf) {
#ifdef _STAT_VER
  return HERMES_POSIX_API->__fxstat(_STAT_VER, fd, stbuf);
#endif
}
}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_POSIX_H
