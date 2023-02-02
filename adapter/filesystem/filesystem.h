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
struct AdapterStat : public IoClientStat {
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

/** A structure to represent IO options */
struct IoOptions : public IoClientOptions {
  hapi::PlacementPolicy dpe_;     /**< data placement policy */
  bool seek_;                     /**< use seek? */
  bool with_fallback_;            /**< use fallback? */

  /** Default constructor */
  IoOptions()
      : dpe_(hapi::PlacementPolicy::kNone),
        seek_(true),
        with_fallback_(true) {}

  /** return options with \a dpe parallel data placement engine */
  static IoOptions WithParallelDpe(hapi::PlacementPolicy dpe) {
    IoOptions opts;
    opts.dpe_ = dpe;
    return opts;
  }

  /** return direct IO options by setting placement policy to none */
  static IoOptions DirectIo(IoOptions &cur_opts) {
    IoOptions opts(cur_opts);
    opts.seek_ = false;
    opts.dpe_ = hapi::PlacementPolicy::kNone;
    opts.with_fallback_ = true;
    return opts;
  }

  /** return IO options with \a mpi_type MPI data type */
  static IoOptions DataType(MPI_Datatype mpi_type, bool seek = true) {
    IoOptions opts;
    opts.mpi_type_ = mpi_type;
    opts.seek_ = seek;
    return opts;
  }

  /**
   * Ensure that I/O goes only to Hermes, and does not fall back to PFS.
   *
   * @param orig_opts The original options to modify
   * */
  static IoOptions PlaceInHermes(IoOptions &orig_opts) {
    IoOptions opts(orig_opts);
    opts.seek_ = false;
    opts.with_fallback_ = false;
    return opts;
  }
};

/** A class to represent file system */
class Filesystem {
 public:
  FilesystemIoClient *io_client_;

 public:
  /** Constructor */
  explicit Filesystem(FilesystemIoClient *io_client) : io_client_(io_client) {}

  /** open \a path */
  File Open(AdapterStat &stat, const std::string &path);
  /** open \a f File in \a path*/
  void Open(AdapterStat &stat, File &f, const std::string &path);

  /** write */
  size_t Write(File &f, AdapterStat &stat, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               IoOptions opts = IoOptions());
  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t off, size_t total_size,
              IoStatus &io_status, IoOptions opts = IoOptions());

  /** write asynchronously */
  HermesRequest *AWrite(File &f, AdapterStat &stat, const void *ptr, size_t off,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        IoOptions opts = IoOptions());
  /** read asynchronously */
  HermesRequest *ARead(File &f, AdapterStat &stat, void *ptr, size_t off,
                       size_t total_size, size_t req_id, IoStatus &io_status,
                       IoOptions opts = IoOptions());
  /** wait for \a req_id request ID */
  size_t Wait(uint64_t req_id);
  /** wait for request IDs in \a req_id vector */
  void Wait(std::vector<uint64_t> &req_id, std::vector<size_t> &ret);
  /** seek */
  off_t Seek(File &f, AdapterStat &stat, SeekMode whence, off_t offset);
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
               IoStatus &io_status, IoOptions opts);
  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr, size_t total_size,
              IoStatus &io_status, IoOptions opts);
  /** write asynchronously */
  HermesRequest *AWrite(File &f, AdapterStat &stat, const void *ptr,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        IoOptions opts);
  /** read asynchronously */
  HermesRequest *ARead(File &f, AdapterStat &stat, void *ptr, size_t total_size,
                       size_t req_id, IoStatus &io_status, IoOptions opts);

  /*
   * Locates the AdapterStat data structure internally, and
   * call the underlying APIs which take AdapterStat as input.
   */

 public:
  /** write */
  size_t Write(File &f, bool &stat_exists, const void *ptr, size_t total_size,
               IoStatus &io_status, IoOptions opts = IoOptions());
  /** read */
  size_t Read(File &f, bool &stat_exists, void *ptr, size_t total_size,
              IoStatus &io_status, IoOptions opts = IoOptions());
  /** write \a off offset */
  size_t Write(File &f, bool &stat_exists, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               IoOptions opts = IoOptions());
  /** read \a off offset */
  size_t Read(File &f, bool &stat_exists, void *ptr, size_t off,
              size_t total_size, IoStatus &io_status,
              IoOptions opts = IoOptions());
  /** write asynchronously */
  HermesRequest *AWrite(File &f, bool &stat_exists, const void *ptr,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        IoOptions opts);
  /** read asynchronously */
  HermesRequest *ARead(File &f, bool &stat_exists, void *ptr, size_t total_size,
                       size_t req_id, IoStatus &io_status, IoOptions opts);
  /** write \a off offset asynchronously */
  HermesRequest *AWrite(File &f, bool &stat_exists, const void *ptr, size_t off,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        IoOptions opts);
  /** read \a off offset asynchronously */
  HermesRequest *ARead(File &f, bool &stat_exists, void *ptr, size_t off,
                       size_t total_size, size_t req_id, IoStatus &io_status,
                       IoOptions opts);
  /** seek */
  off_t Seek(File &f, bool &stat_exists, SeekMode whence, off_t offset);
  /** tell */
  off_t Tell(File &f, bool &stat_exists);
  /** sync */
  int Sync(File &f, bool &stat_exists);
  /** close */
  int Close(File &f, bool &stat_exists, bool destroy = true);

 public:
  /** real open */
  void RealOpen(IoClientContext &f,
                IoClientStat &stat,
                const std::string &path) {
    io_client_->RealOpen(f, stat, path);
  }

  /**
   * Called after real open. Allocates the Hermes representation of
   * identifying file information, such as a hermes file descriptor
   * and hermes file handler. These are not the same as POSIX file
   * descriptor and STDIO file handler.
   * */
  virtual void HermesOpen(IoClientContext &f,
                          IoClientStat &stat,
                          FilesystemIoClientContext &fs_mdm) {
    io_client_->HermesOpen(f, stat, fs_mdm);
  }

  /** real sync */
  int RealSync(const IoClientContext &f, const AdapterStat &stat) {
    return io_client_->RealSync(f, stat);
  }

  /** real close */
  int RealClose(const IoClientContext &f, const AdapterStat &stat) {
    return io_client_->RealClose(f, stat);
  }

 public:
  /** Whether or not \a path PATH is tracked by Hermes */
  static bool IsTracked(const std::string &path) {
    if (!HERMES->IsInitialized()) {
      return false;
    }
    stdfs::path stdfs_path(path);
    // TODO(llogan): use weak_canonical for performance reasons
    std::string abs_path = stdfs::canonical(stdfs_path);
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
    return true;
  }
};

}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_
