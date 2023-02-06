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
#include "adapter/mapper/balanced_mapper.h"
#include <filesystem>
#include <limits>

namespace stdfs = std::filesystem;

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
    hermes_fd_cur_ = hermes_fd_min_;
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
struct FilesystemIoClientObject {
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
  FilesystemIoClientObject(FsIoClientMetadata *mdm, void *stat)
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

  /** Update backend statistics when registering a blob */
  void RegisterBlob(const IoClientContext &opts,
                    GlobalIoClientState &stat) override {
    stat.true_size_ = std::max(stat.true_size_,
                               opts.backend_off_ + opts.backend_size_);
  }

  /** Update backend statistics when unregistering a blob */
  void UnregisterBlob(const IoClientContext &opts,
                      GlobalIoClientState &stat) override {}

  /** Decode I/O client context from the original blob name */
  IoClientContext DecodeBlobName(const IoClientContext &opts,
                                 const std::string &blob_name) override {
    IoClientContext decode_opts;
    BlobPlacement p;
    p.DecodeBlobName(blob_name);
    decode_opts.type_ = opts.type_;
    decode_opts.backend_off_ = p.page_ * p.blob_size_;
    return decode_opts;
  }

  /** real open */
  virtual void RealOpen(IoClientObject &f,
                        IoClientStats &stat,
                        const std::string &path) = 0;

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as POSIX file
   * descriptor and STDIO file handler.
   * */
  virtual void HermesOpen(IoClientObject &f,
                          const IoClientStats &stat,
                          FilesystemIoClientObject &fs_mdm) = 0;

  /** real sync */
  virtual int RealSync(const IoClientObject &f,
                       const IoClientStats &stat) = 0;

  /** real close */
  virtual int RealClose(const IoClientObject &f,
                        IoClientStats &stat) = 0;

  /**
   * Called before RealClose. Releases information provisioned during
   * the allocation phase.
   * */
  virtual void HermesClose(IoClientObject &f,
                           const IoClientStats &stat,
                           FilesystemIoClientObject &fs_mdm) = 0;
};

}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_IO_CLIENT_H_
