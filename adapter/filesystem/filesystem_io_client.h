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

#ifndef HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_
#define HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_

#include "adapter/io_client/io_client.h"
#include <experimental/filesystem>
#include <limits>

namespace stdfs = std::experimental::filesystem;

namespace hermes::adapter::fs {

/**
 * Metadta required by Filesystem I/O clients to perform a HermesOpen
 * */
struct FsIoClientMetadata {
  int hermes_fd_min_, hermes_fd_max_;  /**< Min and max fd values (inclusive)*/
  std::atomic<int> hermes_fd_cur_;     /**< Current fd */

  /** Default constructor */
  FsIoClientMetadata() {
    hermes_fd_min_ = 8192;  // TODO(llogan): don't assume 8192
    hermes_fd_max_ = std::numeric_limits<int>::max();
  }

  /** Allocate a Hermes FD */
  int AllocateFd() {
    int cur = hermes_fd_cur_.fetch_add(1);
    return cur;
  }

  /** Release a Hermes FD */
  void ReleaseFd(int hermes_fd) {
    // TODO(llogan): recycle instead of ignore
    (void) hermes_fd;
  }
};

/**
 * State required by Filesystem I/O clients to perform a HermesOpen
 * */
struct FilesystemIoClientContext {
  /**
   * A pointer to the FsIoClientMetadata stored in the Filesystem
   * */
  FsIoClientMetadata *mdm_;

  /**
   * A pointer to the Adapter Stat object. Used by STDIO + MPI-IO to
   * represent the hermes_fh_ and hermes_mpi_fh_ fields.
   * */
  void *stat_;

  /** Default constructor */
  FilesystemIoClientContext(FsIoClientMetadata *mdm, void *stat)
  : mdm_(mdm), stat_(stat) {}
};

/**
 * Defines I/O clients which are compatible with the filesystem
 * base class.
 * */
class FilesystemIoClient : public IoClient {
 public:
  /** virtual destructor */
  virtual ~FilesystemIoClient() = default;

  /** real open */
  virtual void RealOpen(IoClientContext &f,
                        IoClientStat &stat,
                        const std::string &path) = 0;

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as POSIX file
   * descriptor and STDIO file handler.
   * */
  virtual void HermesOpen(IoClientContext &f,
                          const IoClientStat &stat,
                          FilesystemIoClientContext &fs_mdm) = 0;

  /** real sync */
  virtual int RealSync(const IoClientContext &f,
                       const IoClientStat &stat) = 0;

  /** real close */
  virtual int RealClose(const IoClientContext &f,
                        const IoClientStat &stat) = 0;
};

}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_
