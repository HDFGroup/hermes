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
#include <glog/logging.h>
#include <experimental/filesystem>

#include "hermes_types.h"
#include "singleton.h"
#include "adapter_utils.h"
#include "posix_api.h"
#include "posix_fs_api.h"
#include "posix_singleton_macros.h"
#include "filesystem/filesystem.h"

using hermes::Singleton;
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::IoStatus;
using hermes::adapter::fs::File;

namespace hapi = hermes::api;
namespace stdfs = std::experimental::filesystem;

extern "C" {

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
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept open for filename: " << path
              << " and mode: " << flags << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags_ = flags;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).fd_;
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
  if (flags & O_CREAT) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept open64 for filename: " << path
              << " and mode: " << flags << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags_ = flags;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).fd_;
  }
  if (flags & O_CREAT) {
    return real_api->open64(path, flags, mode);
  }
  return real_api->open64(path, flags);
}

int HERMES_DECL(__open_2)(const char *path, int oflag) {
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept __open_2 for filename: " << path
              << " and mode: " << oflag << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags_ = oflag;
    stat.st_mode_ = 0;
    return fs_api->Open(stat, path).fd_;
  }
  return real_api->__open_2(path, oflag);
}

int HERMES_DECL(creat)(const char *path, mode_t mode) {
  std::string path_str(path);
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept creat for filename: " << path
              << " and mode: " << mode << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags_ = O_CREAT;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).fd_;
  }
  return real_api->creat(path, mode);
}

int HERMES_DECL(creat64)(const char *path, mode_t mode) {
  std::string path_str(path);
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept creat64 for filename: " << path
              << " and mode: " << mode << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags_ = O_CREAT;
    stat.st_mode_ = mode;
    return fs_api->Open(stat, path).fd_;
  }
  return real_api->creat64(path, mode);
}

ssize_t HERMES_DECL(read)(int fd, void *buf, size_t count) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept read." << std::endl;
    File f; f.fd_ = fd; fs_api->_InitFile(f); IoStatus io_status;
    size_t ret = fs_api->Read(f, stat_exists, buf, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->read(fd, buf, count);
}

ssize_t HERMES_DECL(write)(int fd, const void *buf, size_t count) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept write." << std::endl;
    File f; f.fd_ = fd; fs_api->_InitFile(f); IoStatus io_status;
    size_t ret = fs_api->Write(f, stat_exists, buf, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->write(fd, buf, count);
}

ssize_t HERMES_DECL(pread)(int fd, void *buf, size_t count, off_t offset) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept pread." << std::endl;
    File f; f.fd_ = fd; fs_api->_InitFile(f); IoStatus io_status;
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
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f); IoStatus io_status;
    LOG(INFO) << "Intercept pwrite." << std::endl;
    size_t ret = fs_api->Write(f, stat_exists, buf, offset, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->pwrite(fd, buf, count, offset);
}

ssize_t HERMES_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f); IoStatus io_status;
    LOG(INFO) << "Intercept pread64." << std::endl;
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
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f); IoStatus io_status;
    LOG(INFO) << "Intercept pwrite." << std::endl;
    size_t ret = fs_api->Write(f, stat_exists, buf, offset, count, io_status);
    if (stat_exists) return ret;
  }
  return real_api->pwrite64(fd, buf, count, offset);
}

off_t HERMES_DECL(lseek)(int fd, off_t offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f);
    LOG(INFO) << "Intercept lseek offset:" << offset << " whence:" << whence
              << "." << std::endl;
    return fs_api->Seek(f, stat_exists,
                        static_cast<SeekMode>(whence), offset);
  }
  return real_api->lseek(fd, offset, whence);
}

off64_t HERMES_DECL(lseek64)(int fd, off64_t offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f);
    LOG(INFO) << "Intercept lseek64 offset:" << offset << " whence:" << whence
              << "." << std::endl;
    return fs_api->Seek(f, stat_exists,
                        static_cast<SeekMode>(whence), offset);
  }
  return real_api->lseek64(fd, offset, whence);
}

int HERMES_DECL(__fxstat)(int __ver, int fd, struct stat *buf) {
  int result = 0;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f);
    LOG(INFO) << "Intercepted fstat." << std::endl;
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto existing = mdm->Find(f);
    if (existing.second) {
      AdapterStat &astat = existing.first;
      // TODO(chogan): st_dev and st_ino need to be assigned by us, but
      // currently we get them by calling the real fstat on open.
      buf->st_dev = 0;
      buf->st_ino = 0;
      buf->st_mode = astat.st_mode_;
      buf->st_nlink = 0;
      buf->st_uid = astat.st_uid_;
      buf->st_gid = astat.st_gid_;
      buf->st_rdev = 0;
      buf->st_size = astat.st_size_;
      buf->st_blksize = astat.st_blksize_;
      buf->st_blocks = 0;
      buf->st_atime = astat.st_atime_;
      buf->st_mtime = astat.st_mtime_;
      buf->st_ctime = astat.st_ctime_;
    } else {
      result = -1;
      errno = EBADF;
      LOG(ERROR) << "File with descriptor " << fd
                 << " does not exist in Hermes\n";
    }
  } else {
    result = real_api->__fxstat(__ver, fd, buf);
  }
  return result;
}

int HERMES_DECL(fsync)(int fd) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    File f; f.fd_ = fd; fs_api->_InitFile(f);
    LOG(INFO) << "Intercept fsync." << std::endl;
    return fs_api->Sync(f, stat_exists);
  }
  return real_api->fsync(fd);
}

int HERMES_DECL(close)(int fd) {
  bool stat_exists;
  auto real_api = HERMES_POSIX_API;
  auto fs_api = HERMES_POSIX_FS;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept close(" << std::to_string(fd) << ")";
    DLOG(INFO) << " -> " << hermes::adapter::GetFilenameFromFD(fd);
    LOG(INFO) << std::endl;

    File f; f.fd_ = fd; fs_api->_InitFile(f);
    return fs_api->Close(f, stat_exists);
  }
  return real_api->close(fd);
}

}  // extern C
