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
#include <dlfcn.h>
#include <iostream>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#define REQUIRE_API(api_name) \
  if (api_name == nullptr) { \
    LOG(FATAL) << "HERMES Adapter failed to map symbol: " \
    #api_name << std::endl; \
    std::exit(1); \
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
typedef int (*fsync_t)(int fd);
typedef int (*close_t)(int fd);
}

namespace hermes::adapter::fs {

/** Pointers to the real posix API */
class PosixApi {
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
  /** fsync */
  fsync_t fsync = nullptr;
  /** close */
  close_t close = nullptr;

  PosixApi() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "posix_intercepted");
    if (is_intercepted) {
      open = (open_t)dlsym(RTLD_NEXT, "open");
    } else {
      open = (open_t)dlsym(RTLD_DEFAULT, "open");
    }
    REQUIRE_API(open)
    if (is_intercepted) {
      open64 = (open64_t)dlsym(RTLD_NEXT, "open64");
    } else {
      open64 = (open64_t)dlsym(RTLD_DEFAULT, "open64");
    }
    REQUIRE_API(open64)
    if (is_intercepted) {
      __open_2 = (__open_2_t)dlsym(RTLD_NEXT, "__open_2");
    } else {
      __open_2 = (__open_2_t)dlsym(RTLD_DEFAULT, "__open_2");
    }
    REQUIRE_API(__open_2)
    if (is_intercepted) {
      creat = (creat_t)dlsym(RTLD_NEXT, "creat");
    } else {
      creat = (creat_t)dlsym(RTLD_DEFAULT, "creat");
    }
    REQUIRE_API(creat)
    if (is_intercepted) {
      creat64 = (creat64_t)dlsym(RTLD_NEXT, "creat64");
    } else {
      creat64 = (creat64_t)dlsym(RTLD_DEFAULT, "creat64");
    }
    REQUIRE_API(creat64)
    if (is_intercepted) {
      read = (read_t)dlsym(RTLD_NEXT, "read");
    } else {
      read = (read_t)dlsym(RTLD_DEFAULT, "read");
    }
    REQUIRE_API(read)
    if (is_intercepted) {
      write = (write_t)dlsym(RTLD_NEXT, "write");
    } else {
      write = (write_t)dlsym(RTLD_DEFAULT, "write");
    }
    REQUIRE_API(write)
    if (is_intercepted) {
      pread = (pread_t)dlsym(RTLD_NEXT, "pread");
    } else {
      pread = (pread_t)dlsym(RTLD_DEFAULT, "pread");
    }
    REQUIRE_API(pread)
    if (is_intercepted) {
      pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
    } else {
      pwrite = (pwrite_t)dlsym(RTLD_DEFAULT, "pwrite");
    }
    REQUIRE_API(pwrite)
    if (is_intercepted) {
      pread64 = (pread64_t)dlsym(RTLD_NEXT, "pread64");
    } else {
      pread64 = (pread64_t)dlsym(RTLD_DEFAULT, "pread64");
    }
    REQUIRE_API(pread64)
    if (is_intercepted) {
      pwrite64 = (pwrite64_t)dlsym(RTLD_NEXT, "pwrite64");
    } else {
      pwrite64 = (pwrite64_t)dlsym(RTLD_DEFAULT, "pwrite64");
    }
    REQUIRE_API(pwrite64)
    if (is_intercepted) {
      lseek = (lseek_t)dlsym(RTLD_NEXT, "lseek");
    } else {
      lseek = (lseek_t)dlsym(RTLD_DEFAULT, "lseek");
    }
    REQUIRE_API(lseek)
    if (is_intercepted) {
      lseek64 = (lseek64_t)dlsym(RTLD_NEXT, "lseek64");
    } else {
      lseek64 = (lseek64_t)dlsym(RTLD_DEFAULT, "lseek64");
    }
    REQUIRE_API(lseek64)
    if (is_intercepted) {
      __fxstat = (__fxstat_t)dlsym(RTLD_NEXT, "__fxstat");
    } else {
      __fxstat = (__fxstat_t)dlsym(RTLD_DEFAULT, "__fxstat");
    }
    REQUIRE_API(__fxstat)
    if (is_intercepted) {
      fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");
    } else {
      fsync = (fsync_t)dlsym(RTLD_DEFAULT, "fsync");
    }
    REQUIRE_API(fsync)
    if (is_intercepted) {
      close = (close_t)dlsym(RTLD_NEXT, "close");
    } else {
      close = (close_t)dlsym(RTLD_DEFAULT, "close");
    }
    REQUIRE_API(close)
  }
};
}  // namespace hermes::adapter::fs

#undef REQUIRE_API

#endif  // HERMES_ADAPTER_POSIX_H
