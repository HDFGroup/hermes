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

#include <bucket.h>
#include <buffer_pool.h>
#include <ftw.h>
#include <hermes_types.h>
#include <mpi.h>
#include <traits.h>
#include <vbucket.h>

#include <set>
#include <string>

#include "enumerations.h"
#include "mapper/mapper_factory.h"

namespace hapi = hermes::api;

namespace hermes::adapter::fs {

const char kStringDelimiter = '#';
const MapperType kMapperType = MapperType::BALANCED;
FlushingMode global_flushing_mode;
const int kNumThreads = 1;

enum class SeekMode {
  kNone = -1,
  kSet = SEEK_SET,
  kCurrent = SEEK_CUR,
  kEnd = SEEK_END
};

/**
 A structure to represent adapter stat.
*/
struct AdapterStat {
  std::shared_ptr<hapi::Bucket> st_bkid; /**< bucket associated with the file */
  /** VBucket for persisting data asynchronously. */
  std::shared_ptr<hapi::VBucket> st_vbkt;
  /** Blob for locking when coordination is needed. */
  std::string main_lock_blob;
  /** Used for async flushing. */
  std::shared_ptr<hapi::PersistTrait> st_persist;
  std::set<std::string, bool (*)(const std::string &, const std::string &)>
      st_blobs;         /**< Blobs access in the bucket */
  i32 ref_count;        /**< # of time process opens a file */
  int flags;            /**< open() flags for POSIX */
  mode_t st_mode;       /**< protection */
  uid_t st_uid;         /**< user ID of owner */
  gid_t st_gid;         /**< group ID of owner */
  size_t st_size;       /**< total size, in bytes */
  off_t st_ptr;         /**< Current ptr of FILE */
  blksize_t st_blksize; /**< blocksize for blob within bucket */
  timespec st_atim;     /**< time of last access */
  timespec st_mtim;     /**< time of last modification */
  timespec st_ctim;     /**< time of last status change */
  std::string mode_str; /**< mode used for fopen() */

  bool is_append; /**< File is in append mode */
  int amode;      /**< access mode */
  MPI_Info info;  /**< Info object (handle) */
  MPI_Comm comm;  /**< Communicator for the file.*/
  bool atomicity; /**< Consistency semantics for data-access */

  AdapterStat()
      : st_bkid(),
        st_blobs(CompareBlobs),
        ref_count(1),
        flags(0),
        st_mode(),
        st_uid(),
        st_gid(),
        st_size(0),
        st_ptr(0),
        st_blksize(4096),
        st_atim(),
        st_mtim(),
        st_ctim(),
        is_append(false),
        amode(0),
        comm(MPI_COMM_SELF),
        atomicity(false) {}
  /** compare \a a BLOB and \a b BLOB.*/
  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

/**
   A structure to represent file
*/
struct File {
  int fd_;          /**< file descriptor */
  FILE *fh_;        /**< file handler */
  MPI_File mpi_fh_; /**< MPI file handler */

  dev_t st_dev;    /**< device */
  ino_t st_ino;    /**< inode */
  bool status_;    /**< status */
  int mpi_status_; /**< MPI status */

  /** default file constructor */
  File()
      : fd_(-1),
        fh_(nullptr),
        mpi_fh_(nullptr),
        st_dev(-1),
        st_ino(-1),
        status_(true),
        mpi_status_(MPI_SUCCESS) {}

  /** file constructor that copies \a old file */
  File(const File &old) { Copy(old); }

  /** file assignment operator that copies \a old file */
  File &operator=(const File &old) {
    Copy(old);
    return *this;
  }

  /** copy \a old file */
  void Copy(const File &old) {
    fd_ = old.fd_;
    fh_ = old.fh_;
    mpi_fh_ = old.mpi_fh_;
    st_dev = old.st_dev;
    st_ino = old.st_ino;
    status_ = old.status_;
  }

  /** file comparison operator */
  bool operator==(const File &old) const {
    return (st_dev == old.st_dev) && (st_ino == old.st_ino) &&
           (mpi_fh_ == old.mpi_fh_);
  }

  /** return hash value of this class  */
  std::size_t hash() const {
    std::size_t result;
    std::size_t h1 = std::hash<dev_t>{}(st_dev);
    std::size_t h2 = std::hash<ino_t>{}(st_ino);
    std::size_t h3 = std::hash<MPI_File>{}(mpi_fh_);
    result = h1 ^ h2 ^ h3;
    return result;
  }
};

/**
   A structure to represent IO options
*/
struct IoOptions {
  PlacementPolicy dpe_;   /**< data placement policy */
  bool coordinate_;       /**< use coordinate? */
  bool seek_;             /**< use seek? */
  bool with_fallback_;    /**< use fallback? */
  MPI_Datatype mpi_type_; /**< MPI data type */
  int count_;             /**< option count */
  IoOptions()
      : dpe_(PlacementPolicy::kNone),
        coordinate_(true),
        seek_(true),
        with_fallback_(true),
        mpi_type_(MPI_CHAR),
        count_(0) {}

  /** return options with \a dpe parallel data placement engine */
  static IoOptions WithParallelDpe(PlacementPolicy dpe) {
    IoOptions opts;
    opts.dpe_ = dpe;
    opts.coordinate_ = true;
    return opts;
  }

  /** return direct IO options by setting placement policy to none */
  static IoOptions DirectIo(IoOptions &cur_opts) {
    IoOptions opts(cur_opts);
    opts.seek_ = false;
    opts.dpe_ = PlacementPolicy::kNone;
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
};

/**
   A structure to represent IO status
*/
struct IoStatus {
  int mpi_ret_;                /**< MPI return value */
  MPI_Status mpi_status_;      /**< MPI status */
  MPI_Status *mpi_status_ptr_; /**< MPI status pointer */

  IoStatus() : mpi_ret_(MPI_SUCCESS), mpi_status_ptr_(&mpi_status_) {}
};

/**
   A structure to represent Hermes request
*/
struct HermesRequest {
  std::future<size_t> return_future; /**< future result of async op. */
  IoStatus io_status;                /**< IO status */
};

/**
 A structure to represent BLOB placement iterator
*/
struct BlobPlacementIter {
  File &f_;                            /**< file */
  AdapterStat &stat_;                  /**< adapter stat */
  const std::string &filename_;        /**< file name */
  const BlobPlacement &p_;             /**< BLOB placement */
  std::shared_ptr<hapi::Bucket> &bkt_; /**< bucket*/
  IoStatus &io_status_;                /**< IO status */
  IoOptions &opts_;                    /**< IO options */

  std::string blob_name_; /**< BLOB name */
  u8 *mem_ptr_;           /**< pointer to memory  */
  size_t blob_start_;     /**< BLOB start  */
  hapi::Context ctx_;     /**< context  */
  hapi::Blob blob_;       /**< BLOB */
  int rank_;              /**< MPI rank */
  int nprocs_;            /**< number of processes */
  bool blob_exists_;      /**< Does BLOB exist? */

  /** iterate \a p BLOB placement */
  explicit BlobPlacementIter(File &f, AdapterStat &stat,
                             const std::string &filename,
                             const BlobPlacement &p,
                             std::shared_ptr<hapi::Bucket> &bkt,
                             IoStatus &io_status, IoOptions &opts)
      : f_(f),
        stat_(stat),
        filename_(filename),
        p_(p),
        bkt_(bkt),
        io_status_(io_status),
        opts_(opts) {}
};

/**
   A class to represent file system
*/
class Filesystem {
 public:
  /** open \a path */
  File Open(AdapterStat &stat, const std::string &path);
  /** open \a f File in \a path*/
  void Open(AdapterStat &stat, File &f, const std::string &path);
  /** write */
  size_t Write(File &f, AdapterStat &stat, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               IoOptions opts = IoOptions());
  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr, size_t off,
              size_t total_size, IoStatus &io_status,
              IoOptions opts = IoOptions());
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
   * APIs used internally
   * */

 private:
  /** coordinated put */
  void _CoordinatedPut(BlobPlacementIter &wi);
  /** uncoordinated put */
  void _UncoordinatedPut(BlobPlacementIter &wi);
  /** write to a new aligned buffer */
  void _WriteToNewAligned(BlobPlacementIter &write_iter);
  /** write to a new unaligned buffer */
  void _WriteToNewUnaligned(BlobPlacementIter &write_iter);
  /** write to an existing aligned buffer */
  void _WriteToExistingAligned(BlobPlacementIter &write_iter);
  /** write to an existing unaligned buffer */
  void _WriteToExistingUnaligned(BlobPlacementIter &write_iter);
  /** put with fallback */
  void _PutWithFallback(AdapterStat &stat, const std::string &blob_name,
                        const std::string &filename, u8 *data, size_t size,
                        size_t offset, IoStatus &io_status_, IoOptions &opts);
  /** read existing contained buffer */
  size_t _ReadExistingContained(BlobPlacementIter &read_iter);
  /** read existing partial buffer */
  size_t _ReadExistingPartial(BlobPlacementIter &read_iter);
  /** read new buffer */
  size_t _ReadNew(BlobPlacementIter &read_iter);

  void _OpenInitStatsInternal(AdapterStat &stat, bool bucket_exists) {
    // TODO(llogan): This isn't really parallel-safe.
    /**
     * Here we assume that the file size can only be growing.
     * If the bucket already exists and has content not already in
     * the file (e.g., when using ADAPTER_MODE=SCRATCH), we should
     * use the size of the bucket instead.
     *
     * There are other concerns with what happens during multi-tenancy.
     * What happens if one process is opening a file, while another
     * process is adding content? The mechanics here aren't
     * well-defined.
     * */
    if (bucket_exists) {
      size_t bkt_size = stat.st_bkid->GetTotalBlobSize();
      stat.st_size = std::max(bkt_size, stat.st_size);
      LOG(INFO) << "Since bucket exists, should reset its size to: "
                << stat.st_size << std::endl;
    }
    if (stat.is_append) {
      stat.st_ptr = stat.st_size;
    }
  }

  /*
   * The APIs to overload
   */
 public:
  /** initialize file */
  virtual void _InitFile(File &f) = 0;

 private:
  /** open initial status */
  virtual void _OpenInitStats(File &f, AdapterStat &stat) = 0;
  /** real open */
  virtual File _RealOpen(AdapterStat &stat, const std::string &path) = 0;
  /** real write */
  virtual size_t _RealWrite(const std::string &filename, off_t offset,
                            size_t size, const u8 *data_ptr,
                            IoStatus &io_status, IoOptions &opts) = 0;
  /** real read */
  virtual size_t _RealRead(const std::string &filename, off_t offset,
                           size_t size, u8 *data_ptr, IoStatus &io_status,
                           IoOptions &opts) = 0;
  /** io status */
  virtual void _IoStats(size_t count, IoStatus &io_status, IoOptions &opts) {
    (void)count;
    (void)io_status;
    (void)opts;
  }
  /** real sync */
  virtual int _RealSync(File &f) = 0;
  /** real close */
  virtual int _RealClose(File &f) = 0;

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
};

}  // namespace hermes::adapter::fs

namespace std {
/**
   A structure to represent hash
*/
template <>
struct hash<hermes::adapter::fs::File> {
  /** hash creator functor */
  std::size_t operator()(const hermes::adapter::fs::File &key) const {
    return key.hash();
  }
};
}  // namespace std

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_
