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

// Dynamically checked to see which are the real APIs and which are intercepted
bool posix_intercepted = true;

#include <fcntl.h>
#include <stdarg.h>
#include <sys/stat.h>
#include "hermes_shm/util/logging.h"
#include <filesystem>

#include "hermes/hermes_types.h"
#include "hermes_shm/util/singleton.h"

#include "posix_api.h"
#include "posix_fs_api.h"
#include "hermes_adapters/filesystem/filesystem.h"

using hermes::adapter::MetadataManager;
using hermes::adapter::SeekMode;
using hermes::adapter::AdapterStat;
using hermes::adapter::File;
using hermes::adapter::IoStatus;
using hermes::adapter::FsIoOptions;

namespace stdfs = std::filesystem;

extern "C" {

//static __attribute__((constructor(101))) void init_posix(void) {
//  HERMES_POSIX_API;
//  HERMES_POSIX_FS;
//  TRANSPARENT_HERMES();;
//}

/**
 * POSIX
 */
int HERMES_DECL(open)(const char *path, int flags, ...) {
  int mode = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (flags & O_CREAT || flags & O_TMPFILE) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  if (real_api->IsInterceptorLoaded()) {
    TRANSPARENT_HERMES();
  }
  if (real_api->IsInterceptorLoaded() && fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercept open for filename: {}"
          " and mode: {}"
          " is tracked.", path, flags)
    AdapterStat stat;
    stat.flags_ = flags;
    stat.st_mode_ = mode;
    auto f = fs_api->Open(stat, path);
    return f.hermes_fd_;
  }

  if (flags & O_CREAT || flags & O_TMPFILE) {
    return real_api->open(path, flags, mode);
  }
  return real_api->open(path, flags);
}

int HERMES_DECL(open64)(const char *path, int flags, ...) {
  int mode = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (flags & O_CREAT || flags & O_TMPFILE) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  if (real_api->IsInterceptorLoaded()) {
    TRANSPARENT_HERMES();
  }
  if (real_api->IsInterceptorLoaded() && fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercept open64 for filename: {}"
          " and mode: {}"
          " is tracked.", path, flags)
    AdapterStat stat;
    stat.flags_ = flags;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).hermes_fd_;
  }
  if (flags & O_CREAT || flags & O_TMPFILE) {
    return real_api->open64(path, flags, mode);
  }
  return real_api->open64(path, flags);
}

int HERMES_DECL(__open_2)(const char *path, int oflag) {
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (real_api->IsInterceptorLoaded()) {
    TRANSPARENT_HERMES();
  }
  if (real_api->IsInterceptorLoaded() && fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercept __open_2 for filename: {}"
          " and mode: {}"
          " is tracked.", path, oflag)
    AdapterStat stat;
    stat.flags_ = oflag;
    stat.st_mode_ = 0;
    return fs_api->Open(stat, path).hermes_fd_;
  }
  return real_api->__open_2(path, oflag);
}

int HERMES_DECL(creat)(const char *path, mode_t mode) {
  std::string path_str(path);
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (real_api->IsInterceptorLoaded()) {
    TRANSPARENT_HERMES();
  }
  if (real_api->IsInterceptorLoaded() && fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercept creat for filename: {}"
          " and mode: {}"
          " is tracked.", path, mode)
    AdapterStat stat;
    stat.flags_ = O_CREAT;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).hermes_fd_;
  }
  return real_api->creat(path, mode);
}

int HERMES_DECL(creat64)(const char *path, mode_t mode) {
  std::string path_str(path);
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (real_api->IsInterceptorLoaded()) {
    TRANSPARENT_HERMES();
  }
  if (real_api->IsInterceptorLoaded() && fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercept creat64 for filename: {}"
          " and mode: {}"
          " is tracked.", path, mode)
    AdapterStat stat;
    stat.flags_ = O_CREAT;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).hermes_fd_;
  }
  return real_api->creat64(path, mode);
}

ssize_t HERMES_DECL(read)(int fd, void *buf, size_t count) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercept read.");
    File f; f.hermes_fd_ = fd; IoStatus io_status;
    size_t ret = fs_api->Read(f, stat_exists, buf, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->read(fd, buf, count);
}

ssize_t HERMES_DECL(write)(int fd, const void *buf, size_t count) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercept write.");
    File f; f.hermes_fd_ = fd; IoStatus io_status;
    size_t ret = fs_api->Write(f, stat_exists, buf, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->write(fd, buf, count);
}

ssize_t HERMES_DECL(pread)(int fd, void *buf, size_t count, off_t offset) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercept pread.");
    File f; f.hermes_fd_ = fd; IoStatus io_status;
    size_t ret = fs_api->Read(f, stat_exists, buf, offset, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->pread(fd, buf, count, offset);
}

ssize_t HERMES_DECL(pwrite)(int fd, const void *buf, size_t count,
                            off_t offset) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    File f; f.hermes_fd_ = fd; IoStatus io_status;
    HILOG(kDebug, "Intercept pwrite.");
    size_t ret = fs_api->Write(f, stat_exists, buf, offset, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->pwrite(fd, buf, count, offset);
}

ssize_t HERMES_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    File f; f.hermes_fd_ = fd; IoStatus io_status;
    HILOG(kDebug, "Intercept pread64.");
    size_t ret = fs_api->Read(f, stat_exists, buf, offset, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->pread64(fd, buf, count, offset);
}

ssize_t HERMES_DECL(pwrite64)(int fd, const void *buf, size_t count,
                              off64_t offset) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    File f; f.hermes_fd_ = fd; IoStatus io_status;
    HILOG(kDebug, "Intercept pwrite64.");
    size_t ret = fs_api->Write(f, stat_exists, buf, offset, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->pwrite64(fd, buf, count, offset);
}

off_t HERMES_DECL(lseek)(int fd, off_t offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    File f; f.hermes_fd_ = fd;
    HILOG(kDebug, "Intercept lseek offset: {} whence: {}.",
          offset, whence)
    return fs_api->Seek(f, stat_exists,
                        static_cast<SeekMode>(whence), offset);
  }
  return real_api->lseek(fd, offset, whence);
}

off64_t HERMES_DECL(lseek64)(int fd, off64_t offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    File f; f.hermes_fd_ = fd;
    HILOG(kDebug, "Intercept lseek64 offset: {} whence: {}.",
          offset, whence)
    return fs_api->Seek(f, stat_exists,
                        static_cast<SeekMode>(whence), offset);
  }
  return real_api->lseek64(fd, offset, whence);
}

int HERMES_DECL(__fxstat)(int __ver, int fd, struct stat *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercepted __fxstat.")
    File f; f.hermes_fd_ = fd;
    result = fs_api->Stat(f, buf);
  } else {
    result = real_api->__fxstat(__ver, fd, buf);
  }
  return result;
}

int HERMES_DECL(__fxstatat)(int __ver, int __fildes,
                            const char *__filename,
                            struct stat *__stat_buf, int __flag) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(__filename)) {
    HILOG(kDebug, "Intercepted __fxstatat.")
    result = fs_api->Stat(__filename, __stat_buf);
  } else {
    result = real_api->__fxstatat(__ver, __fildes, __filename,
                                  __stat_buf, __flag);
  }
  return result;
}

int HERMES_DECL(__xstat)(int __ver, const char *__filename,
                         struct stat *__stat_buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(__filename)) {
    // HILOG(kDebug, "Intercepted __xstat for file {}.", __filename)
    result = fs_api->Stat(__filename, __stat_buf);
  } else {
    result = real_api->__xstat(__ver, __filename, __stat_buf);
  }
  return result;
}

int HERMES_DECL(__lxstat)(int __ver, const char *__filename,
                          struct stat *__stat_buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(__filename)) {
    HILOG(kDebug, "Intercepted __lxstat.")
    result = fs_api->Stat(__filename, __stat_buf);
  } else {
    result = real_api->__lxstat(__ver, __filename, __stat_buf);
  }
  return result;
}

int HERMES_DECL(fstat)(int fd, struct stat *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercepted fstat.")
    File f; f.hermes_fd_ = fd;
    result = fs_api->Stat(f, buf);
  } else {
    result = real_api->fstat(fd, buf);
  }
  return result;
}

int HERMES_DECL(stat)(const char *pathname, struct stat *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(pathname)) {
    HILOG(kDebug, "Intercepted stat.")
    result = fs_api->Stat(pathname, buf);
  } else {
    result = real_api->stat(pathname, buf);
  }
  return result;
}

int HERMES_DECL(__fxstat64)(int __ver, int fd, struct stat64 *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercepted __fxstat.")
    File f; f.hermes_fd_ = fd;
    result = fs_api->Stat(f, buf);
  } else {
    result = real_api->__fxstat64(__ver, fd, buf);
  }
  return result;
}

int HERMES_DECL(__fxstatat64)(int __ver, int __fildes,
                            const char *__filename,
                            struct stat64 *__stat_buf, int __flag) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(__filename)) {
    HILOG(kDebug, "Intercepted __fxstatat.")
    result = fs_api->Stat(__filename, __stat_buf);
  } else {
    result = real_api->__fxstatat64(__ver, __fildes, __filename,
                                  __stat_buf, __flag);
  }
  return result;
}

int HERMES_DECL(__xstat64)(int __ver, const char *__filename,
                         struct stat64 *__stat_buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(__filename)) {
    HILOG(kDebug, "Intercepted __xstat.")
    result = fs_api->Stat(__filename, __stat_buf);
  } else {
    result = real_api->__xstat64(__ver, __filename, __stat_buf);
  }
  return result;
}

int HERMES_DECL(__lxstat64)(int __ver, const char *__filename,
                          struct stat64 *__stat_buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(__filename)) {
    HILOG(kDebug, "Intercepted __lxstat.")
    result = fs_api->Stat(__filename, __stat_buf);
  } else {
    result = real_api->__lxstat64(__ver, __filename, __stat_buf);
  }
  return result;
}

int HERMES_DECL(fstat64)(int fd, struct stat64 *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercepted fstat.")
    File f; f.hermes_fd_ = fd;
    result = fs_api->Stat(f, buf);
  } else {
    result = real_api->fstat64(fd, buf);
  }
  return result;
}

int HERMES_DECL(stat64)(const char *pathname, struct stat64 *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(pathname)) {
    HILOG(kDebug, "Intercepted stat.")
    result = fs_api->Stat(pathname, buf);
  } else {
    result = real_api->stat64(pathname, buf);
  }
  return result;
}

int HERMES_DECL(fsync)(int fd) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    File f; f.hermes_fd_ = fd;
    HILOG(kDebug, "Intercepted fsync.")
    return fs_api->Sync(f, stat_exists);
  }
  return real_api->fsync(fd);
}

int HERMES_DECL(close)(int fd) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercepted close({}).",
          fd)
    File f; f.hermes_fd_ = fd;
    return fs_api->Close(f, stat_exists);
  }
  return real_api->close(fd);
}

int HERMES_DECL(flock)(int fd, int operation) {
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsFdTracked(fd)) {
    HILOG(kDebug, "Intercepted flock({}).",
          fd)
    // TODO(llogan): implement?
    return 0;
  }
  return real_api->close(fd);
}

int HERMES_DECL(remove)(const char *pathname) {
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(pathname)) {
    HILOG(kDebug, "Intercepted remove({})", pathname)
    return fs_api->Remove(pathname);
  }
  return real_api->remove(pathname);
}

int HERMES_DECL(unlink)(const char *pathname) {
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (fs_api->IsPathTracked(pathname)) {
    HILOG(kDebug, "Intercepted unlink({})", pathname)
    return fs_api->Remove(pathname);
  }
  return real_api->unlink(pathname);
}

}  // extern C
