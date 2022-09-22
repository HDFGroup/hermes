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
#include <string>
#include <bucket.h>
#include <buffer_pool.h>
#include <hermes_types.h>
#include "mapper/mapper_factory.h"

namespace hapi = hermes::api;

namespace hermes::adapter::fs {

struct AdapterStat {
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the file */
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
        st_ctim() {} /* default constructor */

  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

struct File {
  int fd_;
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
    st_dev = old.st_dev;
    st_ino = old.st_ino;
    status_ = old.status_;
  }

  bool operator==(const File &old) const {
    return fd_ == old.fd_;
  }

  std::size_t hash() const {
    std::size_t result;
    std::size_t h1 = std::hash<dev_t>{}(st_dev);
    std::size_t h2 = std::hash<ino_t>{}(st_ino);
    result = h1 ^ (h2 << 1);
    return result;
  }
};

struct BlobPlacementIter {
  File &f_;
  AdapterStat &stat_;
  const std::string &filename_;
  const BlobPlacement &p_;
  std::shared_ptr<hapi::Bucket> &bkt_;

  u8 *mem_ptr_;
  size_t blob_start_;
  hapi::Context ctx_;
  hapi::Blob blob_;

  explicit BlobPlacementIter(File &f, AdapterStat &stat,
                             const std::string &filename,
                             const BlobPlacement &p,
                             std::shared_ptr<hapi::Bucket> &bkt) :
        f_(f), stat_(stat), filename_(filename), p_(p), bkt_(bkt) {}
};

class Filesystem {
 public:
  File Open(AdapterStat &stat, const std::string &path);
  size_t Write(File &f, AdapterStat &stat, const void *ptr,
               size_t total_size,
               PlacementPolicy dpe = PlacementPolicy::kNone);
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t total_size,
              PlacementPolicy dpe = PlacementPolicy::kNone);
  size_t Write(File &f, AdapterStat &stat, const void *ptr,
               size_t off, size_t total_size, bool seek = false,
               PlacementPolicy dpe = PlacementPolicy::kNone);
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t off, size_t total_size, bool seek = false,
              PlacementPolicy dpe = PlacementPolicy::kNone);
  off_t Seek(File &f, AdapterStat &stat, int whence, off_t offset);
  int Sync(File &f, AdapterStat &stat);
  int Close(File &f, AdapterStat &stat, bool destroy = true);

  virtual void _InitFile(File &f) = 0;

 private:
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
  size_t Write(File &f, bool &stat_exists, const void *ptr,
               size_t total_size,
               PlacementPolicy dpe = PlacementPolicy::kNone);
  size_t Read(File &f, bool &stat_exists, void *ptr,
              size_t total_size,
              PlacementPolicy dpe = PlacementPolicy::kNone);
  size_t Write(File &f, bool &stat_exists, const void *ptr,
               size_t off, size_t total_size, bool seek = false,
               PlacementPolicy dpe = PlacementPolicy::kNone);
  size_t Read(File &f, bool &stat_exists, void *ptr,
              size_t off, size_t total_size, bool seek = false,
              PlacementPolicy dpe = PlacementPolicy::kNone);
  off_t Seek(File &f, bool &stat_exists, int whence, off_t offset);
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
