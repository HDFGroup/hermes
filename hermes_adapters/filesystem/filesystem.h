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

#ifndef O_TMPFILE
#define O_TMPFILE 0x0
#endif

#include <ftw.h>
#include <mpi.h>
#include <future>
#include <set>
#include <string>

#include "hermes/bucket.h"
#include "hermes/hermes.h"

#include "filesystem_io_client.h"
#include <filesystem>


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

/** A class to represent file system */
class Filesystem {
 public:
  FilesystemIoClient *io_client_;
  AdapterType type_;

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
  Task* AWrite(File &f, AdapterStat &stat, const void *ptr, size_t off,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts = FsIoOptions());
  /** read asynchronously */
  Task* ARead(File &f, AdapterStat &stat, void *ptr, size_t off,
                       size_t total_size, size_t req_id, IoStatus &io_status,
                       FsIoOptions opts = FsIoOptions());
  /** wait for \a req_id request ID */
  size_t Wait(uint64_t req_id);
  /** wait for request IDs in \a req_id vector */
  void Wait(std::vector<uint64_t> &req_id, std::vector<size_t> &ret);
  /** seek */
  off_t Seek(File &f, AdapterStat &stat, SeekMode whence, off64_t offset);
  /** file size */
  size_t GetSize(File &f, AdapterStat &stat);
  /** tell */
  off_t Tell(File &f, AdapterStat &stat);
  /** sync */
  int Sync(File &f, AdapterStat &stat);
  /** truncate */
  int Truncate(File &f, AdapterStat &stat, size_t new_size);
  /** close */
  int Close(File &f, AdapterStat &stat);
  /** remove */
  int Remove(const std::string &pathname);

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
  Task* AWrite(File &f, AdapterStat &stat, const void *ptr,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts);
  /** read asynchronously */
  Task* ARead(File &f, AdapterStat &stat, void *ptr, size_t total_size,
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
  Task* AWrite(File &f, bool &stat_exists, const void *ptr,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts);
  /** read asynchronously */
  Task* ARead(File &f, bool &stat_exists, void *ptr, size_t total_size,
                       size_t req_id, IoStatus &io_status, FsIoOptions opts);
  /** write \a off offset asynchronously */
  Task* AWrite(File &f, bool &stat_exists, const void *ptr, size_t off,
                        size_t total_size, size_t req_id, IoStatus &io_status,
                        FsIoOptions opts);
  /** read \a off offset asynchronously */
  Task* ARead(File &f, bool &stat_exists, void *ptr, size_t off,
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
  /** truncate */
  int Truncate(File &f, bool &stat_exists, size_t new_size);
  /** close */
  int Close(File &f, bool &stat_exists);

 public:
  /** Whether or not \a path PATH is tracked by Hermes */
  static bool IsPathTracked(const std::string &path) {
    if (!HERMES_CONF->is_initialized_) {
      return false;
    }
    std::string abs_path = stdfs::absolute(path).string();
    auto &paths = HERMES_CLIENT_CONF.path_list_;
    // Check if path is included or excluded
    for (config::UserPathInfo &pth : paths) {
      if (abs_path.rfind(pth.path_) != std::string::npos) {
        if (abs_path == pth.path_ && pth.is_directory_) {
          // Do not include if path is a tracked directory
          return false;
        }
        return pth.include_;
      }
    }
    // Assume it is excluded
    return false;
  }
};

}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_H_
