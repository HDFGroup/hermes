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

#include "enumerations.h"
#include <ftw.h>
#include <string>
#include <bucket.h>
#include <buffer_pool.h>
#include <hermes_types.h>
#include <traits.h>
#include "mapper/mapper_factory.h"
#include <mpi.h>

namespace hapi = hermes::api;

namespace hermes::adapter::fs {

const char kStringDelimiter = '#';
const MapperType kMapperType = MapperType::BALANCED;
FlushingMode global_flushing_mode;

struct AdapterStat {
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the file */
  /** VBucket for persisting data asynchronously. */
  std::shared_ptr<hapi::VBucket> st_vbkt;
  /** Used for async flushing. */
  std::shared_ptr<hapi::PersistTrait> st_persist;
  std::set<std::string, bool (*)(const std::string &, const std::string &)>
      st_blobs;         /* Blobs access in the bucket */
  i32 ref_count;        /* # of time process opens a file */
  int flags;            /* open() flags for POSIX */
  mode_t st_mode;       /* protection */
  uid_t st_uid;         /* user ID of owner */
  gid_t st_gid;         /* group ID of owner */
  off_t st_size;        /* total size, in bytes */
  off_t st_ptr;         /* Current ptr of FILE */
  blksize_t st_blksize; /* blocksize for blob within bucket */
  timespec st_atim;     /* time of last access */
  timespec st_mtim;     /* time of last modification */
  timespec st_ctim;     /* time of last status change */
  std::string mode_str; /* mode used for fopen() */

  int a_mode;                            /* access mode */
  MPI_Info info;                         /* Info object (handle) */
  MPI_Comm comm;                         /* Communicator for the file.*/
  MPI_Offset size;                       /* total size, in bytes */
  MPI_Offset ptr;                        /* Current ptr of FILE */
  bool atomicity; /* Consistency semantics for data-access */

  AdapterStat()
      : st_bkid(),
        st_blobs(CompareBlobs),
        ref_count(1),
        st_mode(),
        st_uid(),
        st_gid(),
        st_size(0),
        st_ptr(0),
        st_blksize(4096),
        st_atim(),
        st_mtim(),
        st_ctim(),
        atomicity(false) {} /* default constructor */

  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

struct File {
  int fd_;
  FILE *fh_;
  dev_t st_dev;
  ino_t st_ino;
  bool status_;

  File() : fd_(-1),
           st_dev(-1),
           st_ino(-1),
           status_(true) {}

  File(const File &old) {
    Copy(old);
  }

  File &operator=(const File &old) {
    Copy(old);
    return *this;
  }

  void Copy(const File &old) {
    fd_ = old.fd_;
    fh_ = old.fh_;
    st_dev = old.st_dev;
    st_ino = old.st_ino;
    status_ = old.status_;
  }

  bool operator==(const File &old) const {
    return (st_dev == old.st_dev) && (st_ino == old.st_ino);
  }

  std::size_t hash() const {
    std::size_t result;
    std::size_t h1 = std::hash<dev_t>{}(st_dev);
    std::size_t h2 = std::hash<ino_t>{}(st_ino);
    result = h1 ^ h2;
    return result;
  }
};

struct IoOptions {
  PlacementPolicy dpe_;
  bool coordinate_;
  MPI_Comm comm_;
  bool seek_;
  IoOptions() :
                dpe_(PlacementPolicy::kNone),
                coordinate_(false),
                comm_(MPI_COMM_WORLD),
                seek_(true) {}

  static IoOptions WithDpe(PlacementPolicy dpe) {
    IoOptions opts;
    opts.dpe_ = dpe;
    return std::move(opts);
  }
};

struct BlobPlacementIter {
  File &f_;
  AdapterStat &stat_;
  const std::string &filename_;
  const BlobPlacement &p_;
  std::shared_ptr<hapi::Bucket> &bkt_;
  IoOptions &opts_;

  std::string blob_name_;
  u8 *mem_ptr_;
  size_t blob_start_;
  hapi::Context ctx_;
  hapi::Blob blob_;
  int rank_;
  int nprocs_;
  bool blob_exists_;

  explicit BlobPlacementIter(File &f, AdapterStat &stat,
                             const std::string &filename,
                             const BlobPlacement &p,
                             std::shared_ptr<hapi::Bucket> &bkt,
                             IoOptions &opts) :
        f_(f), stat_(stat), filename_(filename),
        p_(p), bkt_(bkt), opts_(opts) {}
};

class Filesystem {
 public:
  File Open(AdapterStat &stat, const std::string &path);
  void Open(AdapterStat &stat, File &f, const std::string &path);
  size_t Write(File &f, AdapterStat &stat, const void *ptr,
               size_t off, size_t total_size,
               IoOptions opts = IoOptions());
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t off, size_t total_size,
              IoOptions opts = IoOptions());
  off_t Seek(File &f, AdapterStat &stat, int whence, off_t offset);
  off_t Tell(File &f, AdapterStat &stat);
  int Sync(File &f, AdapterStat &stat);
  int Close(File &f, AdapterStat &stat, bool destroy = true);

  virtual void _InitFile(File &f) = 0;

 private:
  void _CoordinatedPut(BlobPlacementIter &wi);
  void _UncoordinatedPut(BlobPlacementIter &wi);
  void _WriteToNewAligned(BlobPlacementIter &write_iter);
  void _WriteToNewUnaligned(BlobPlacementIter &write_iter);
  void _WriteToExistingAligned(BlobPlacementIter &write_iter);
  void _WriteToExistingUnaligned(BlobPlacementIter &write_iter);
  void _PutWithFallback(AdapterStat &stat, const std::string &blob_name,
                        const std::string &filename, u8 *data, size_t size,
                        size_t offset);
  size_t _ReadExistingContained(BlobPlacementIter &read_iter);
  size_t _ReadExistingPartial(BlobPlacementIter &read_iter);
  size_t _ReadNew(BlobPlacementIter &read_iter);

  virtual void _OpenInitStats(File &f, AdapterStat &stat,
                              bool bucket_exists) = 0;
  virtual File _RealOpen(AdapterStat &stat, const std::string &path) = 0;
  virtual size_t _RealWrite(const std::string &filename, off_t offset,
                            size_t size, u8 *data_ptr) = 0;
  virtual size_t _RealRead(const std::string &filename, off_t offset,
                           size_t size, u8 *data_ptr) = 0;
  virtual int _RealSync(File &f) = 0;
  virtual int _RealClose(File &f) = 0;

 public:
  size_t Write(File &f, AdapterStat &stat, const void *ptr,
               size_t total_size, IoOptions opts);
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t total_size, IoOptions opts);
  size_t Write(File &f, bool &stat_exists, const void *ptr,
               size_t total_size, IoOptions opts = IoOptions());
  size_t Read(File &f, bool &stat_exists, void *ptr,
              size_t total_size, IoOptions opts = IoOptions());
  size_t Write(File &f, bool &stat_exists, const void *ptr,
               size_t off, size_t total_size, IoOptions opts = IoOptions());
  size_t Read(File &f, bool &stat_exists, void *ptr,
              size_t off, size_t total_size, IoOptions opts = IoOptions());
  off_t Seek(File &f, bool &stat_exists, int whence, off_t offset);
  off_t Tell(File &f, bool &stat_exists);
  int Sync(File &f, bool &stat_exists);
  int Close(File &f, bool &stat_exists, bool destroy = true);
};

}  // namespace hermes::adapter::fs

namespace std {
template <>
struct hash<hermes::adapter::fs::File> {
  std::size_t operator()(const hermes::adapter::fs::File &key) const {
    return key.hash();
  }
};
}  // namespace std

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_
