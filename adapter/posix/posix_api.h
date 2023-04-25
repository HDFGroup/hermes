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
#include "adapter/real_api.h"

#ifndef O_TMPFILE
#define O_TMPFILE 0
#endif

#ifndef _STAT_VER
#define _STAT_VER 0
#endif

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

// stat functions
typedef int (*__fxstat_t)(int __ver, int __filedesc, struct stat * __stat_buf);
typedef int (*__xstat_t)(int __ver, const char *__filename,
                         struct stat *__stat_buf);
typedef int (*__lxstat_t)(int __ver, const char *__filename,
                          struct stat *__stat_buf);
typedef int (*__fxstatat_t)(int __ver, int __fildes, const char *__filename,
                            struct stat *__stat_buf, int __flag);
typedef int (*stat_t)(const char * pathname, struct stat * __stat_buf);
typedef int (*fstat_t)(int __filedesc, struct stat * __stat_buf);

// fxstat functions
typedef int (*__fxstat64_t)(int __ver, int __fildes, struct stat64 *__stat_buf);
typedef int (*__xstat64_t)(int __ver, const char *__filename,
                     struct stat64 *__stat_buf);
typedef int (*__lxstat64_t)(int __ver, const char *__filename,
                      struct stat64 *__stat_buf);
typedef int (*__fxstatat64_t)(int __ver, int __fildes, const char *__filename,
                        struct stat64 *__stat_buf, int __flag);
typedef int (*stat64_t)(const char * pathname, struct stat64 * __stat_buf);
typedef int (*fstat64_t)(int __filedesc, struct stat64 * __stat_buf);

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
  /** __fxstatat_t */
  __fxstatat_t __fxstatat = nullptr;
  /** __xstat */
  __xstat_t __xstat = nullptr;
  /** __lxstat */
  __lxstat_t __lxstat = nullptr;
  /** fstat */
  fstat_t fstat = nullptr;
  /** stat */
  stat_t stat = nullptr;

  /** __fxstat */
  __fxstat64_t __fxstat64 = nullptr;
  /** __fxstatat_t */
  __fxstatat64_t __fxstatat64 = nullptr;
  /** __xstat */
  __xstat64_t __xstat64 = nullptr;
  /** __lxstat */
  __lxstat64_t __lxstat64 = nullptr;
  /** fstat */
  fstat64_t fstat64 = nullptr;
  /** stat */
  stat64_t stat64 = nullptr;

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
    open = (open_t)dlsym(real_lib_, "open");
    REQUIRE_API(open)
    open64 = (open64_t)dlsym(real_lib_, "open64");
    REQUIRE_API(open64)
    __open_2 = (__open_2_t)dlsym(real_lib_, "__open_2");
    REQUIRE_API(__open_2)
    creat = (creat_t)dlsym(real_lib_, "creat");
    REQUIRE_API(creat)
    creat64 = (creat64_t)dlsym(real_lib_, "creat64");
    REQUIRE_API(creat64)
    read = (read_t)dlsym(real_lib_, "read");
    REQUIRE_API(read)
    write = (write_t)dlsym(real_lib_, "write");
    REQUIRE_API(write)
    pread = (pread_t)dlsym(real_lib_, "pread");
    REQUIRE_API(pread)
    pwrite = (pwrite_t)dlsym(real_lib_, "pwrite");
    REQUIRE_API(pwrite)
    pread64 = (pread64_t)dlsym(real_lib_, "pread64");
    REQUIRE_API(pread64)
    pwrite64 = (pwrite64_t)dlsym(real_lib_, "pwrite64");
    REQUIRE_API(pwrite64)
    lseek = (lseek_t)dlsym(real_lib_, "lseek");
    REQUIRE_API(lseek)
    lseek64 = (lseek64_t)dlsym(real_lib_, "lseek64");
    REQUIRE_API(lseek64)
    // Try to hook a stat function
    __fxstat = (__fxstat_t)dlsym(real_lib_, "__fxstat");
    __fxstatat = (__fxstatat_t)dlsym(real_lib_, "__fxstatat");
    __xstat = (__xstat_t)dlsym(real_lib_, "__xstat");
    __lxstat = (__lxstat_t)dlsym(real_lib_, "__lxstat");
    stat = (stat_t)dlsym(real_lib_, "stat");
    fstat = (fstat_t)dlsym(real_lib_, "fstat");
    REQUIRE_API(fstat || __fxstat || __fxstatat)
    REQUIRE_API(stat || __xstat || __lxstat)
    if (!fstat) {
      // NOTE(llogan): We use real_api->fstat in a couple of places
      // fstat does need to be mapped always, or Hermes may segfault.
      fstat = fxstat_to_fstat;
    }

    // Try to hook a stat64 function
    __fxstat64 = (__fxstat64_t)dlsym(real_lib_, "__fxstat64");
    __fxstatat64 = (__fxstatat64_t)dlsym(real_lib_, "__fxstatat64");
    __xstat64 = (__xstat64_t)dlsym(real_lib_, "__xstat64");
    __lxstat64 = (__lxstat64_t)dlsym(real_lib_, "__lxstat64");
    stat64 = (stat64_t)dlsym(real_lib_, "stat64");
    fstat64 = (fstat64_t)dlsym(real_lib_, "fstat6464");

    fsync = (fsync_t)dlsym(real_lib_, "fsync");
    REQUIRE_API(fsync)
    close = (close_t)dlsym(real_lib_, "close");
    REQUIRE_API(close)
    flock = (flock_t)dlsym(real_lib_, "flock");
    REQUIRE_API(flock)
    remove = (remove_t)dlsym(real_lib_, "remove");
    REQUIRE_API(remove)
    unlink = (unlink_t)dlsym(real_lib_, "unlink");
    REQUIRE_API(unlink)
  }
};

}  // namespace hermes::adapter::fs

// Singleton macros
#include "hermes_shm/util/singleton.h"

#define HERMES_POSIX_API \
  hshm::EasySingleton<hermes::adapter::fs::PosixApi>::GetInstance()
#define HERMES_POSIX_API_T hermes::adapter::fs::PosixApi*

namespace hermes::adapter::fs {
/** Used for compatability with older kernel versions */
static int fxstat_to_fstat(int fd, struct stat *stbuf) {
#ifdef _STAT_VER
  return HERMES_POSIX_API->__fxstat(_STAT_VER, fd, stbuf);
#endif
}
}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_POSIX_H
