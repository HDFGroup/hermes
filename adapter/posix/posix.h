#ifndef HERMES_ADAPTER_POSIX_H
#define HERMES_ADAPTER_POSIX_H
#include <string>
#include <dlfcn.h>
#include <iostream>
#include <glog/logging.h>
#include "interceptor.h"

namespace hermes::adapter::posix {

class API {
 public:
  typedef int(*real_open_t)(const char * path, int flags,  ...);
  int(*real_open_)(const char * path, int flags,  ...) = nullptr;
  typedef int(*real_open64_t)(const char * path, int flags,  ...);
  int(*real_open64_)(const char * path, int flags,  ...) = nullptr;
  typedef int(*real___open_2_t)(const char * path, int oflag);
  int(*real___open_2_)(const char * path, int oflag) = nullptr;
  typedef int(*real_creat_t)(const char * path, mode_t mode);
  int(*real_creat_)(const char * path, mode_t mode) = nullptr;
  typedef int(*real_creat64_t)(const char * path, mode_t mode);
  int(*real_creat64_)(const char * path, mode_t mode) = nullptr;
  typedef ssize_t(*real_read_t)(int fd, void * buf, size_t count);
  ssize_t(*real_read_)(int fd, void * buf, size_t count) = nullptr;
  typedef ssize_t(*real_write_t)(int fd, const void * buf, size_t count);
  ssize_t(*real_write_)(int fd, const void * buf, size_t count) = nullptr;
  typedef ssize_t(*real_pread_t)(int fd, void * buf, size_t count, off_t offset);
  ssize_t(*real_pread_)(int fd, void * buf, size_t count, off_t offset) = nullptr;
  typedef ssize_t(*real_pwrite_t)(int fd, const void * buf, size_t count, off_t offset);
  ssize_t(*real_pwrite_)(int fd, const void * buf, size_t count, off_t offset) = nullptr;
  typedef ssize_t(*real_pread64_t)(int fd, void * buf, size_t count, off64_t offset);
  ssize_t(*real_pread64_)(int fd, void * buf, size_t count, off64_t offset) = nullptr;
  typedef ssize_t(*real_pwrite64_t)(int fd, const void * buf, size_t count, off64_t offset);
  ssize_t(*real_pwrite64_)(int fd, const void * buf, size_t count, off64_t offset) = nullptr;
  typedef off_t(*real_lseek_t)(int fd, off_t offset, int whence);
  off_t(*real_lseek_)(int fd, off_t offset, int whence) = nullptr;
  typedef off64_t(*real_lseek64_t)(int fd, off64_t offset, int whence);
  off64_t(*real_lseek64_)(int fd, off64_t offset, int whence) = nullptr;
  typedef int(*real___fxstat_t)(int version, int fd, struct stat * buf);
  int(*real___fxstat_)(int version, int fd, struct stat * buf) = nullptr;
  typedef int(*real_fsync_t)(int fd);
  int(*real_fsync_)(int fd) = nullptr;
  typedef int(*real_fdatasync_t)(int fd);
  int(*real_fdatasync_)(int fd) = nullptr;
  typedef int(*real_close_t)(int fd);
  int(*real_close_)(int fd) = nullptr;
  typedef int(*real_MPI_Init_t)(int * argc, char *** argv);
  int(*real_MPI_Init_)(int * argc, char *** argv) = nullptr;
  typedef int(*real_MPI_Finalize_t)( );
  int(*real_MPI_Finalize_)( ) = nullptr;
  API() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "posix_intercepted");
    if (is_intercepted) {
      real_open_ = (real_open_t)dlsym(RTLD_NEXT, "open");
    } else {
      real_open_ = (real_open_t)dlsym(RTLD_DEFAULT, "open");
    }
    if (real_open_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "open" << std::endl;
    }

    if (is_intercepted) {
      real_open64_ = (real_open64_t)dlsym(RTLD_NEXT, "open64");
    } else {
      real_open64_ = (real_open64_t)dlsym(RTLD_DEFAULT, "open64");
    }
    if (real_open64_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "open64" << std::endl;
    }

    if (is_intercepted) {
      real___open_2_ = (real___open_2_t)dlsym(RTLD_NEXT, "__open_2");
    } else {
      real___open_2_ = (real___open_2_t)dlsym(RTLD_DEFAULT, "__open_2");
    }
    if (real___open_2_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "__open_2" << std::endl;
    }

    if (is_intercepted) {
      real_creat_ = (real_creat_t)dlsym(RTLD_NEXT, "creat");
    } else {
      real_creat_ = (real_creat_t)dlsym(RTLD_DEFAULT, "creat");
    }
    if (real_creat_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "creat" << std::endl;
    }

    if (is_intercepted) {
      real_creat64_ = (real_creat64_t)dlsym(RTLD_NEXT, "creat64");
    } else {
      real_creat64_ = (real_creat64_t)dlsym(RTLD_DEFAULT, "creat64");
    }
    if (real_creat64_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "creat64" << std::endl;
    }

    if (is_intercepted) {
      real_read_ = (real_read_t)dlsym(RTLD_NEXT, "read");
    } else {
      real_read_ = (real_read_t)dlsym(RTLD_DEFAULT, "read");
    }
    if (real_read_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "read" << std::endl;
    }

    if (is_intercepted) {
      real_write_ = (real_write_t)dlsym(RTLD_NEXT, "write");
    } else {
      real_write_ = (real_write_t)dlsym(RTLD_DEFAULT, "write");
    }
    if (real_write_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "write" << std::endl;
    }

    if (is_intercepted) {
      real_pread_ = (real_pread_t)dlsym(RTLD_NEXT, "pread");
    } else {
      real_pread_ = (real_pread_t)dlsym(RTLD_DEFAULT, "pread");
    }
    if (real_pread_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pread" << std::endl;
    }

    if (is_intercepted) {
      real_pwrite_ = (real_pwrite_t)dlsym(RTLD_NEXT, "pwrite");
    } else {
      real_pwrite_ = (real_pwrite_t)dlsym(RTLD_DEFAULT, "pwrite");
    }
    if (real_pwrite_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pwrite" << std::endl;
    }

    if (is_intercepted) {
      real_pread64_ = (real_pread64_t)dlsym(RTLD_NEXT, "pread64");
    } else {
      real_pread64_ = (real_pread64_t)dlsym(RTLD_DEFAULT, "pread64");
    }
    if (real_pread64_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pread64" << std::endl;
    }

    if (is_intercepted) {
      real_pwrite64_ = (real_pwrite64_t)dlsym(RTLD_NEXT, "pwrite64");
    } else {
      real_pwrite64_ = (real_pwrite64_t)dlsym(RTLD_DEFAULT, "pwrite64");
    }
    if (real_pwrite64_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "pwrite64" << std::endl;
    }

    if (is_intercepted) {
      real_lseek_ = (real_lseek_t)dlsym(RTLD_NEXT, "lseek");
    } else {
      real_lseek_ = (real_lseek_t)dlsym(RTLD_DEFAULT, "lseek");
    }
    if (real_lseek_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "lseek" << std::endl;
    }

    if (is_intercepted) {
      real_lseek64_ = (real_lseek64_t)dlsym(RTLD_NEXT, "lseek64");
    } else {
      real_lseek64_ = (real_lseek64_t)dlsym(RTLD_DEFAULT, "lseek64");
    }
    if (real_lseek64_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "lseek64" << std::endl;
    }

    if (is_intercepted) {
      real___fxstat_ = (real___fxstat_t)dlsym(RTLD_NEXT, "__fxstat");
    } else {
      real___fxstat_ = (real___fxstat_t)dlsym(RTLD_DEFAULT, "__fxstat");
    }
    if (real___fxstat_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "__fxstat" << std::endl;
    }

    if (is_intercepted) {
      real_fsync_ = (real_fsync_t)dlsym(RTLD_NEXT, "fsync");
    } else {
      real_fsync_ = (real_fsync_t)dlsym(RTLD_DEFAULT, "fsync");
    }
    if (real_fsync_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fsync" << std::endl;
    }

    if (is_intercepted) {
      real_fdatasync_ = (real_fdatasync_t)dlsym(RTLD_NEXT, "fdatasync");
    } else {
      real_fdatasync_ = (real_fdatasync_t)dlsym(RTLD_DEFAULT, "fdatasync");
    }
    if (real_fdatasync_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fdatasync" << std::endl;
    }

    if (is_intercepted) {
      real_close_ = (real_close_t)dlsym(RTLD_NEXT, "close");
    } else {
      real_close_ = (real_close_t)dlsym(RTLD_DEFAULT, "close");
    }
    if (real_close_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "close" << std::endl;
    }

    if (is_intercepted) {
      real_MPI_Init_ = (real_MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      real_MPI_Init_ = (real_MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    if (real_MPI_Init_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Init" << std::endl;
    }

    if (is_intercepted) {
      real_MPI_Finalize_ = (real_MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      real_MPI_Finalize_ = (real_MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    if (real_MPI_Finalize_ == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Finalize" << std::endl;
    }

  }

};

}  // namespace hermes::adapter::posix

#endif  // HERMES_ADAPTER_POSIX_H