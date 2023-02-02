//
// Created by lukemartinlogan on 2/1/23.
//

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
    // TODO(llogan): Recycle old fds
    hermes_fd_min_ = 8192;  // TODO(llogan): don't assume 8192
    hermes_fd_max_ = std::numeric_limits<int>::max();
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