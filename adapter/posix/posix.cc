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

#include <experimental/filesystem>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <glog/logging.h>

#include "interceptor.cc"
#include "adapter_utils.cc"
#include "posix/constants.h"

#include <hermes.h>
#include <bucket.h>
#include <vbucket.h>

#include "constants.h"
#include "interceptor.h"
#include "singleton.h"

#include "posix/posix.h"
#include "posix/native.h"

#ifndef O_TMPFILE
#define O_TMPFILE 0
#endif

using hermes::adapter::WeaklyCanonical;
using hermes::adapter::ReadGap;
using hermes::adapter::posix::API;
using hermes::adapter::posix::PosixFS;
using hermes::adapter::Singleton;
using hermes::adapter::fs::MetadataManager;

namespace hapi = hermes::api;
namespace stdfs = std::experimental::filesystem;
using hermes::u8;


/**
 * MPI
 */
int HERMES_DECL(MPI_Init)(int *argc, char ***argv) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  int status = real_api->MPI_Init(argc, argv);
  if (status == 0) {
    LOG(INFO) << "MPI Init intercepted." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->InitializeHermes(true);
  }
  return status;
}

int HERMES_DECL(MPI_Finalize)(void) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  LOG(INFO) << "MPI Finalize intercepted." << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  int status = real_api->MPI_Finalize();
  return status;
}

/**
 * POSIX
 */
int HERMES_DECL(open)(const char *path, int flags, ...) {
  int ret;
  int mode = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
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
    stat.flags = flags;
    stat.st_mode = mode;
    ret = fs_api->Open(stat, path).fd_;
  } else {
    if (flags & O_CREAT || flags & O_TMPFILE) {
      ret = real_api->open(path, flags, mode);
    } else {
      ret = real_api->open(path, flags);
    }
  }
  return (ret);
}

int HERMES_DECL(open64)(const char *path, int flags, ...) {
  int ret;
  int mode = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  if (flags & O_CREAT) {
    va_list arg;
    va_start(arg, flags);
    mode = va_arg(arg, int);
    va_end(arg);
  }
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept open for filename: " << path
              << " and mode: " << flags << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags = flags;
    stat.st_mode = mode;
    ret = fs_api->Open(stat, path).fd_;
  } else {
    if (flags & O_CREAT) {
      ret = real_api->open64(path, flags, mode);
    } else {
      ret = real_api->open64(path, flags);
    }
  }
  return (ret);
}

int HERMES_DECL(__open_2)(const char *path, int oflag) {
  int ret;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept __open_2 for filename: " << path
              << " and mode: " << oflag << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags = oflag;
    stat.st_mode = 0;
    ret = fs_api->Open(stat, path).fd_;
  } else {
    ret = real_api->__open_2(path, oflag);
  }
  return (ret);
}

int HERMES_DECL(creat)(const char *path, mode_t mode) {
  int ret;
  std::string path_str(path);
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept creat for filename: " << path
              << " and mode: " << mode << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags = O_CREAT;
    stat.st_mode = mode;
    ret = fs_api->Open(stat, path).fd_;
  } else {
    ret = real_api->creat(path, mode);
  }
  return (ret);
}

int HERMES_DECL(creat64)(const char *path, mode_t mode) {
  int ret;
  std::string path_str(path);
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercept creat64 for filename: " << path
              << " and mode: " << mode << " is tracked." << std::endl;
    AdapterStat stat;
    stat.flags = O_CREAT;
    stat.st_mode = mode;
    ret = fs_api->Open(stat, path).fd_;
  } else {
    ret = real_api->creat64(path, mode);
  }
  return (ret);
}

ssize_t HERMES_DECL(read)(int fd, void *buf, size_t count) {
  size_t ret;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept read." << std::endl;
      ret = fs_api->Read(f, existing.first, buf, count);
      return (ret);
    }
  }
  return real_api->read(fd, buf, count);
}

ssize_t HERMES_DECL(write)(int fd, const void *buf, size_t count) {
  size_t ret;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept write." << std::endl;
      ret = fs_api->Write(f, existing.first, buf, count);
      return (ret);
    }
  }
  ret = real_api->write(fd, buf, count);
  return (ret);
}

ssize_t HERMES_DECL(pread)(int fd, void *buf, size_t count, off_t offset) {
  size_t ret = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept pread." << std::endl;
      ret = fs_api->Read(f, existing.first, buf, offset, count);
      return ret;
    }
  }
  ret = real_api->pread(fd, buf, count, offset);
  return (ret);
}

ssize_t HERMES_DECL(pwrite)(int fd, const void *buf, size_t count,
                            off_t offset) {
  size_t ret = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept pwrite." << std::endl;
      ret = fs_api->Write(f, existing.first, buf, offset, count);
      return ret;
    }
  }
  ret = real_api->pwrite(fd, buf, count, offset);
  return (ret);
}

ssize_t HERMES_DECL(pread64)(int fd, void *buf, size_t count, off64_t offset) {
  size_t ret = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept pread64." << std::endl;
      ret = fs_api->Read(f, existing.first, buf, offset, count);
      return ret;
    }
  }
  ret = real_api->pread64(fd, buf, count, offset);
  return (ret);
}

ssize_t HERMES_DECL(pwrite64)(int fd, const void *buf, size_t count,
                              off64_t offset) {
  size_t ret = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept pwrite." << std::endl;
      ret = fs_api->Write(f, existing.first, buf, offset, count);
      return ret;
    }
  }
  ret = real_api->pwrite64(fd, buf, count, offset);
  return (ret);
}

off_t HERMES_DECL(lseek)(int fd, off_t offset, int whence) {
  int ret = -1;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept lseek offset:" << offset << " whence:" << whence
                << "." << std::endl;
      ret = fs_api->Seek(f, existing.first, whence, offset);
      return ret;
    }
  }
  ret = real_api->lseek(fd, offset, whence);
  return (ret);
}

off64_t HERMES_DECL(lseek64)(int fd, off64_t offset, int whence) {
  int ret = -1;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "Intercept lseek64 offset:" << offset << " whence:" << whence
                << "." << std::endl;
      ret = fs_api->Seek(f, existing.first, whence, offset);
    }
    return ret;
  }
  ret = real_api->lseek64(fd, offset, whence);
  return (ret);
}

int HERMES_DECL(__fxstat)(int version, int fd, struct stat *buf) {
  int result = 0;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercepted fstat." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      AdapterStat &astat = existing.first;
      // TODO(chogan): st_dev and st_ino need to be assigned by us, but
      // currently we get them by calling the real fstat on open.
      buf->st_dev = 0;
      buf->st_ino = 0;
      buf->st_mode = astat.st_mode;
      buf->st_nlink = 0;
      buf->st_uid = astat.st_uid;
      buf->st_gid = astat.st_gid;
      buf->st_rdev = 0;
      buf->st_size = astat.st_size;
      buf->st_blksize = astat.st_blksize;
      buf->st_blocks = 0;
      buf->st_atime = astat.st_atime;
      buf->st_mtime = astat.st_mtime;
      buf->st_ctime = astat.st_ctime;
    } else {
      result = -1;
      errno = EBADF;
      LOG(ERROR) << "File with descriptor" << fd
                 << "does not exist in Hermes\n";
    }
  } else {
    result = real_api->__fxstat(version, fd, buf);
  }
  return result;
}

int HERMES_DECL(fsync)(int fd) {
  int ret;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept fsync." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      hapi::Context ctx;
      if (existing.first.ref_count == 1) {
        auto persist = INTERCEPTOR_LIST->Persists(fd);
        auto filename = existing.first.st_bkid->GetName();
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty() && persist) {
          LOG(INFO) << "POSIX fsync Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          hapi::VBucket file_vbucket(filename, mdm->GetHermes(), ctx);
          auto offset_map = std::unordered_map<std::string, hermes::u64>();

          for (const auto &blob_name : blob_names) {
            auto status = file_vbucket.Link(blob_name, filename, ctx);
            if (!status.Failed()) {
              auto page_index = std::stol(blob_name) - 1;
              offset_map.emplace(blob_name, page_index * kPageSize);
            }
          }
          bool flush_synchronously = true;
          hapi::PersistTrait persist_trait(filename, offset_map,
                                           flush_synchronously);
          file_vbucket.Attach(&persist_trait, ctx);
          file_vbucket.Destroy(ctx);
          existing.first.st_blobs.clear();
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
          mdm->Update(f, existing.first);
        }
      } else {
        LOG(INFO) << "File handler is opened by more than one fopen."
                  << std::endl;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        existing.first.st_atim = ts;
        existing.first.st_ctim = ts;
        mdm->Update(f, existing.first);
      }
    }
  }
  ret = real_api->fsync(fd);
  return (ret);
}

int HERMES_DECL(close)(int fd) {
  int ret;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<PosixFS>::GetInstance();
  File f; f.fd_ = fd;
  if (hermes::adapter::IsTracked(fd)) {
    LOG(INFO) << "Intercept close(" << std::to_string(fd) << ")";
    DLOG(INFO) << " -> " << hermes::adapter::GetFilenameFromFD(fd);
    LOG(INFO) << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      hapi::Context ctx;
      if (existing.first.ref_count == 1) {
        auto persist = INTERCEPTOR_LIST->Persists(fd);
        auto filename = existing.first.st_bkid->GetName();
        mdm->Delete(f);
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty() && persist) {
          LOG(INFO) << "POSIX close Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
          hapi::VBucket file_vbucket(filename, mdm->GetHermes(), ctx);
          auto offset_map = std::unordered_map<std::string, hermes::u64>();

          for (const auto &blob_name : blob_names) {
            auto status = file_vbucket.Link(blob_name, filename, ctx);
            if (!status.Failed()) {
              auto page_index = std::stol(blob_name) - 1;
              offset_map.emplace(blob_name, page_index * kPageSize);
            }
          }
          bool flush_synchronously = true;
          hapi::PersistTrait persist_trait(filename, offset_map,
                                           flush_synchronously);
          file_vbucket.Attach(&persist_trait, ctx);
          file_vbucket.Destroy(ctx);
          existing.first.st_blobs.clear();
          INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
        }
        existing.first.st_bkid->Destroy(ctx);
        mdm->FinalizeHermes();
      } else {
        LOG(INFO) << "File handler is opened by more than one fopen."
                  << std::endl;
        existing.first.ref_count--;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        existing.first.st_atim = ts;
        existing.first.st_ctim = ts;
        mdm->Update(f, existing.first);
        existing.first.st_bkid->Release(ctx);
      }
    }
  }

  ret = real_api->close(fd);
  return (ret);
}
