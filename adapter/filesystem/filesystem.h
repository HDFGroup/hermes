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

#ifndef HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_
#define HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_

#include <ftw.h>
#include <mpi.h>
#include <future>
#include <set>
#include <string>

#include "traits.h"
#include "bucket.h"
#include "vbucket.h"
#include "hermes.h"

#include "adapter/io_client/io_client.h"
#include "filesystem_io_client.h"
#include "file.h"

#include <filesystem>

namespace hapi = hermes::api;

namespace hermes::adapter::fs {

/** The maximum length of a posix path */
static inline const int kMaxPathLen = 4096;

/** The type of seek to perform */
enum class SeekMode {
  kNone = -1,
  kSet = SEEK_SET,
  kCurrent = SEEK_CUR,
  kEnd = SEEK_END
};

/** A structure to represent adapter statistics */
struct AdapterStat : public IoClientStats {
  std::shared_ptr<hapi::Bucket> bkt_id_; /**< bucket associated with the file */
  /** VBucket for persisting data asynchronously. */
  std::shared_ptr<hapi::VBucket> vbkt_id_;

  /** Default constructor. */
  AdapterStat()
      : bkt_id_(),
        vbkt_id_() {}

  /** compare \a a BLOB and \a b BLOB.*/
  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

/**< Whether to perform seek */
#define HERMES_FS_SEEK (1<< (HERMES_IO_CLIENT_FLAGS_COUNT))

/**
 * A structure to represent IO options for FS adapter.
 * For now, nothing additional than the typical IoClientContext.
 * */
struct FsIoOptions : public IoClientContext {
  /** Default constructor */
  FsIoOptions() : IoClientContext() {
    flags_.SetBits(HERMES_FS_SEEK);
  }

  /** Enable seek for this I/O */
  void SetSeek() {
    flags_.SetBits(HERMES_FS_SEEK);
  }

  /** Disable seek for this I/O */
  void UnsetSeek() {
    flags_.UnsetBits(HERMES_FS_SEEK);
  }

  /** Whether or not to perform seek in FS adapter */
  bool DoSeek() {
    return flags_.OrBits(HERMES_FS_SEEK);
  }

  /** return IO options with \a mpi_type MPI data type */
  static FsIoOptions DataType(MPI_Datatype mpi_type, bool seek = true) {
    FsIoOptions opts;
    opts.mpi_type_ = mpi_type;
    if (!seek) { opts.UnsetSeek(); }
    return opts;
  }
};

/** A class to represent file system */
class Filesystem {
 public:
  FilesystemIoClient *io_client_;
  AdapterType type_;
  Context ctx_;

 public:
  /** Constructor */
  explicit Filesystem(FilesystemIoClient *io_client, AdapterType type)
  : io_client_(io_client), type_(type) {}

  /** open \a path */
  File Open(AdapterStat &stat, const std::string &path);
  /** open \a f File in \a path*/
  void Open(AdapterStat &stat, File &f, const std::string &path);

  /** write */
  size_t Write(File &f, AdapterStat &stat, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               FsIoOptions opts = FsIoOptions());
  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t off, size_t total_size,
              IoStatus &io_status, FsIoOptions opts = FsIoOptions());

  /** write asynchronously */
  HermesRequest *AWrite(File &f, AdapterStat &stat, const void *ptr, size_t off,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts = FsIoOptions());
  /** read asynchronously */
  HermesRequest *ARead(File &f, AdapterStat &stat, void *ptr, size_t off,
                       size_t total_size, size_t req_id, IoStatus &io_status,
                       FsIoOptions opts = FsIoOptions());
  /** wait for \a req_id request ID */
  size_t Wait(uint64_t req_id);
  /** wait for request IDs in \a req_id vector */
  void Wait(std::vector<uint64_t> &req_id, std::vector<size_t> &ret);
  /** seek */
  off_t Seek(File &f, AdapterStat &stat, SeekMode whence, off_t offset);
  /** file size */
  size_t GetSize(File &f, AdapterStat &stat);
  /** tell */
  off_t Tell(File &f, AdapterStat &stat);
  /** sync */
  int Sync(File &f, AdapterStat &stat);
  /** close */
  int Close(File &f, AdapterStat &stat, bool destroy = true);

  /*
   * I/O APIs which seek based on the internal AdapterStat st_ptr,
   * instead of taking an offset as input.
   */

 public:
  /** write */
  size_t Write(File &f, AdapterStat &stat, const void *ptr, size_t total_size,
               IoStatus &io_status, FsIoOptions opts);
  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr, size_t total_size,
              IoStatus &io_status, FsIoOptions opts);
  /** write asynchronously */
  HermesRequest *AWrite(File &f, AdapterStat &stat, const void *ptr,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts);
  /** read asynchronously */
  HermesRequest *ARead(File &f, AdapterStat &stat, void *ptr, size_t total_size,
                       size_t req_id, IoStatus &io_status, FsIoOptions opts);

  /*
   * Locates the AdapterStat data structure internally, and
   * call the underlying APIs which take AdapterStat as input.
   */

 public:
  /** write */
  size_t Write(File &f, bool &stat_exists, const void *ptr, size_t total_size,
               IoStatus &io_status, FsIoOptions opts = FsIoOptions());
  /** read */
  size_t Read(File &f, bool &stat_exists, void *ptr, size_t total_size,
              IoStatus &io_status, FsIoOptions opts = FsIoOptions());
  /** write \a off offset */
  size_t Write(File &f, bool &stat_exists, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               FsIoOptions opts = FsIoOptions());
  /** read \a off offset */
  size_t Read(File &f, bool &stat_exists, void *ptr, size_t off,
              size_t total_size, IoStatus &io_status,
              FsIoOptions opts = FsIoOptions());
  /** write asynchronously */
  HermesRequest *AWrite(File &f, bool &stat_exists, const void *ptr,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts);
  /** read asynchronously */
  HermesRequest *ARead(File &f, bool &stat_exists, void *ptr, size_t total_size,
                       size_t req_id, IoStatus &io_status, FsIoOptions opts);
  /** write \a off offset asynchronously */
  HermesRequest *AWrite(File &f, bool &stat_exists, const void *ptr, size_t off,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts);
  /** read \a off offset asynchronously */
  HermesRequest *ARead(File &f, bool &stat_exists, void *ptr, size_t off,
                       size_t total_size, size_t req_id, IoStatus &io_status,
                       FsIoOptions opts);
  /** seek */
  off_t Seek(File &f, bool &stat_exists, SeekMode whence, off_t offset);
  /** file sizes */
  size_t GetSize(File &f, bool &stat_exists);
  /** tell */
  off_t Tell(File &f, bool &stat_exists);
  /** sync */
  int Sync(File &f, bool &stat_exists);
  /** close */
  int Close(File &f, bool &stat_exists, bool destroy = true);

 public:
  /** real open */
  void RealOpen(IoClientObject &f,
                IoClientStats &stat,
                const std::string &path) {
    io_client_->RealOpen(f, stat, path);
  }

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as POSIX file
   * descriptor and STDIO file handler.
   * */
  virtual void HermesOpen(IoClientObject &f,
                          IoClientStats &stat,
                          FilesystemIoClientObject &fs_mdm) {
    io_client_->HermesOpen(f, stat, fs_mdm);
  }

  /** real sync */
  int RealSync(const IoClientObject &f, const AdapterStat &stat) {
    return io_client_->RealSync(f, stat);
  }

  /** real close */
  int RealClose(const IoClientObject &f, const AdapterStat &stat) {
    return io_client_->RealClose(f, stat);
  }

 public:
  /** Whether or not \a path PATH is tracked by Hermes */
  static bool IsPathTracked(const std::string &path) {
    if (!HERMES->IsInitialized()) {
      return false;
    }
    std::string abs_path = stdfs::weakly_canonical(path).string();
    auto &path_inclusions = HERMES->client_config_.path_inclusions_;
    auto &path_exclusions = HERMES->client_config_.path_exclusions_;
    // Check if path is included
    for (const std::string &pth : path_inclusions) {
      if (abs_path.rfind(pth) != std::string::npos) {
        return true;
      }
    }
    // Check if path is excluded
    for (const std::string &pth : path_exclusions) {
      if (abs_path.rfind(pth) != std::string::npos) {
        return false;
      }
    }
    return false;
  }
};

}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_
