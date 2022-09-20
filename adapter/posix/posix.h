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
#include "interceptor.h"
#include "filesystem/filesystem.h"
#include "filesystem/metadata_manager.h"

namespace hermes::adapter::posix {

class API {
 public:
  typedef int(*open_t)(const char * path, int flags,  ...);
  int(*open)(const char * path, int flags,  ...) = nullptr;
  typedef int(*open64_t)(const char * path, int flags,  ...);
  int(*open64)(const char * path, int flags,  ...) = nullptr;
  typedef int(*__open_2_t)(const char * path, int oflag);
  int(*__open_2)(const char * path, int oflag) = nullptr;
  typedef int(*creat_t)(const char * path, mode_t mode);
  int(*creat)(const char * path, mode_t mode) = nullptr;
  typedef int(*creat64_t)(const char * path, mode_t mode);
  int(*creat64)(const char * path, mode_t mode) = nullptr;
  typedef ssize_t(*read_t)(int fd, void * buf, size_t count);
  ssize_t(*read)(int fd, void * buf, size_t count) = nullptr;
  typedef ssize_t(*write_t)(int fd, const void * buf, size_t count);
  ssize_t(*write)(int fd, const void * buf, size_t count) = nullptr;
  typedef ssize_t(*pread_t)(int fd, void * buf, size_t count, off_t offset);
  ssize_t(*pread)(int fd, void * buf, size_t count, off_t offset) = nullptr;
  typedef ssize_t(*pwrite_t)(int fd, const void * buf, size_t count, off_t offset);
  ssize_t(*pwrite)(int fd, const void * buf, size_t count, off_t offset) = nullptr;
  typedef ssize_t(*pread64_t)(int fd, void * buf, size_t count, off64_t offset);
  ssize_t(*pread64)(int fd, void * buf, size_t count, off64_t offset) = nullptr;
  typedef ssize_t(*pwrite64_t)(int fd, const void * buf, size_t count, off64_t offset);
  ssize_t(*pwrite64)(int fd, const void * buf, size_t count, off64_t offset) = nullptr;
  typedef off_t(*lseek_t)(int fd, off_t offset, int whence);
  off_t(*lseek)(int fd, off_t offset, int whence) = nullptr;
  typedef off64_t(*lseek64_t)(int fd, off64_t offset, int whence);
  off64_t(*lseek64)(int fd, off64_t offset, int whence) = nullptr;
  typedef int(*__fxstat_t)(int version, int fd, struct stat * buf);
  int(*__fxstat)(int version, int fd, struct stat * buf) = nullptr;
  typedef int(*fsync_t)(int fd);
  int(*fsync)(int fd) = nullptr;
  typedef int(*fdatasync_t)(int fd);
  int(*fdatasync)(int fd) = nullptr;
  typedef int(*close_t)(int fd);
  int(*close)(int fd) = nullptr;
  typedef int(*MPI_Init_t)(int * argc, char *** argv);
  int(*MPI_Init)(int * argc, char *** argv) = nullptr;
  typedef int(*MPI_Finalize_t)();
  int(*MPI_Finalize)() = nullptr;
  API() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "posix_intercepted");
    if (is_intercepted) {
      open = (open_t)dlsym(RTLD_NEXT, "open");
    } else {
      open = (open_t)dlsym(RTLD_DEFAULT, "open");
    }
    if (open == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "open" << std::endl;
    }
    if (is_intercepted) {
      open64 = (open64_t)dlsym(RTLD_NEXT, "open64");
    } else {
      open64 = (open64_t)dlsym(RTLD_DEFAULT, "open64");
    }
    if (open64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "open64" << std::endl;
    }
    if (is_intercepted) {
      __open_2 = (__open_2_t)dlsym(RTLD_NEXT, "__open_2");
    } else {
      __open_2 = (__open_2_t)dlsym(RTLD_DEFAULT, "__open_2");
    }
    if (__open_2 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "__open_2" << std::endl;
    }
    if (is_intercepted) {
      creat = (creat_t)dlsym(RTLD_NEXT, "creat");
    } else {
      creat = (creat_t)dlsym(RTLD_DEFAULT, "creat");
    }
    if (creat == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "creat" << std::endl;
    }
    if (is_intercepted) {
      creat64 = (creat64_t)dlsym(RTLD_NEXT, "creat64");
    } else {
      creat64 = (creat64_t)dlsym(RTLD_DEFAULT, "creat64");
    }
    if (creat64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "creat64" << std::endl;
    }
    if (is_intercepted) {
      read = (read_t)dlsym(RTLD_NEXT, "read");
    } else {
      read = (read_t)dlsym(RTLD_DEFAULT, "read");
    }
    if (read == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "read" << std::endl;
    }
    if (is_intercepted) {
      write = (write_t)dlsym(RTLD_NEXT, "write");
    } else {
      write = (write_t)dlsym(RTLD_DEFAULT, "write");
    }
    if (write == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "write" << std::endl;
    }
    if (is_intercepted) {
      pread = (pread_t)dlsym(RTLD_NEXT, "pread");
    } else {
      pread = (pread_t)dlsym(RTLD_DEFAULT, "pread");
    }
    if (pread == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pread" << std::endl;
    }
    if (is_intercepted) {
      pwrite = (pwrite_t)dlsym(RTLD_NEXT, "pwrite");
    } else {
      pwrite = (pwrite_t)dlsym(RTLD_DEFAULT, "pwrite");
    }
    if (pwrite == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pwrite" << std::endl;
    }
    if (is_intercepted) {
      pread64 = (pread64_t)dlsym(RTLD_NEXT, "pread64");
    } else {
      pread64 = (pread64_t)dlsym(RTLD_DEFAULT, "pread64");
    }
    if (pread64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pread64" << std::endl;
    }
    if (is_intercepted) {
      pwrite64 = (pwrite64_t)dlsym(RTLD_NEXT, "pwrite64");
    } else {
      pwrite64 = (pwrite64_t)dlsym(RTLD_DEFAULT, "pwrite64");
    }
    if (pwrite64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pwrite64" << std::endl;
    }
    if (is_intercepted) {
      lseek = (lseek_t)dlsym(RTLD_NEXT, "lseek");
    } else {
      lseek = (lseek_t)dlsym(RTLD_DEFAULT, "lseek");
    }
    if (lseek == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "lseek" << std::endl;
    }
    if (is_intercepted) {
      lseek64 = (lseek64_t)dlsym(RTLD_NEXT, "lseek64");
    } else {
      lseek64 = (lseek64_t)dlsym(RTLD_DEFAULT, "lseek64");
    }
    if (lseek64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "lseek64" << std::endl;
    }
    if (is_intercepted) {
      __fxstat = (__fxstat_t)dlsym(RTLD_NEXT, "__fxstat");
    } else {
      __fxstat = (__fxstat_t)dlsym(RTLD_DEFAULT, "__fxstat");
    }
    if (__fxstat == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "__fxstat" << std::endl;
    }
    if (is_intercepted) {
      fsync = (fsync_t)dlsym(RTLD_NEXT, "fsync");
    } else {
      fsync = (fsync_t)dlsym(RTLD_DEFAULT, "fsync");
    }
    if (fsync == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fsync" << std::endl;
    }
    if (is_intercepted) {
      fdatasync = (fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");
    } else {
      fdatasync = (fdatasync_t)dlsym(RTLD_DEFAULT, "fdatasync");
    }
    if (fdatasync == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fdatasync" << std::endl;
    }
    if (is_intercepted) {
      close = (close_t)dlsym(RTLD_NEXT, "close");
    } else {
      close = (close_t)dlsym(RTLD_DEFAULT, "close");
    }
    if (close == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "close" << std::endl;
    }
    if (is_intercepted) {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    if (MPI_Init == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Init" << std::endl;
    }
    if (is_intercepted) {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    if (MPI_Finalize == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Finalize" << std::endl;
    }
  }
};
}  // namespace hermes::adapter::posix

#endif  // HERMES_ADAPTER_POSIX_H
