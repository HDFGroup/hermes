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

#include "filesystem_mdm.h"
#include "filesystem_io_client.h"
#include "hermes_adapters/mapper/mapper_factory.h"
#include "data_stager/factory/binary_stager.h"
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
class Filesystem : public FilesystemIoClient {
 public:
  AdapterType type_;

 public:
  /** Constructor */
  explicit Filesystem(AdapterType type)
      : type_(type) {}

  /** open \a path */
  File Open(AdapterStat &stat, const std::string &path) {
    File f;
    auto mdm = HERMES_FS_METADATA_MANAGER;
    if (stat.adapter_mode_ == AdapterMode::kNone) {
      stat.adapter_mode_ = mdm->GetAdapterMode(path);
    }
    RealOpen(f, stat, path);
    if (!f.status_) {
      return f;
    }
    Open(stat, f, path);
    return f;
  }

  /** open \a f File in \a path*/
  void Open(AdapterStat &stat, File &f, const std::string &path) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);

    std::shared_ptr<AdapterStat> exists = mdm->Find(f);
    if (!exists) {
      HILOG(kDebug, "File not opened before by adapter")
      // Normalize path strings
      stat.path_ = stdfs::absolute(path).string();
      auto path_shm = hipc::make_uptr<hipc::charbuf>(stat.path_);
      // Verify the bucket exists if not in CREATE mode
      if (stat.adapter_mode_ == AdapterMode::kScratch &&
          !stat.hflags_.Any(HERMES_FS_EXISTS) &&
          !stat.hflags_.Any(HERMES_FS_CREATE)) {
        TagId bkt_id = HERMES->GetTagId(stat.path_);
        if (bkt_id.IsNull()) {
          f.status_ = false;
          return;
        }
      }
      // Update page size
      stat.page_size_ = mdm->GetAdapterPageSize(path);
      // Bucket parameters
      ctx.bkt_params_ = hermes::data_stager::BinaryFileStager::BuildFileParams(stat.page_size_);
      // Get or create the bucket
      if (stat.hflags_.Any(HERMES_FS_TRUNC)) {
        // The file was opened with TRUNCATION
        stat.bkt_id_ = HERMES->GetBucket(stat.path_, ctx, 0, HERMES_SHOULD_STAGE);
        stat.bkt_id_.Clear();
      } else {
        // The file was opened regularly
        stat.file_size_ = GetBackendSize(*path_shm);
        stat.bkt_id_ = HERMES->GetBucket(stat.path_, ctx, stat.file_size_, HERMES_SHOULD_STAGE);
      }
      HILOG(kDebug, "BKT vs file size: {} {}", stat.bkt_id_.GetSize(), stat.file_size_);
      // Update file position pointer
      if (stat.hflags_.Any(HERMES_FS_APPEND)) {
        stat.st_ptr_ = std::numeric_limits<size_t>::max();
      } else {
        stat.st_ptr_ = 0;
      }
      // Allocate internal hermes data
      auto stat_ptr = std::make_shared<AdapterStat>(stat);
      FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void*)stat_ptr.get());
      HermesOpen(f, stat, fs_ctx);
      mdm->Create(f, stat_ptr);
    } else {
      HILOG(kDebug, "File already opened by adapter")
      exists->UpdateTime();
    }
  }

  /** write */
  size_t Write(File &f, AdapterStat &stat, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               FsIoOptions opts = FsIoOptions()) {
    (void) f;
    hapi::Bucket &bkt = stat.bkt_id_;
    std::string filename = bkt.GetName();
    bool is_append = stat.st_ptr_ == std::numeric_limits<size_t>::max();

    HILOG(kDebug, "Write called for filename: {}"
                  " on offset: {}"
                  " from position: {}"
                  " and size: {}"
                  " and adapter mode: {}",
          filename, off, stat.st_ptr_, total_size,
          AdapterModeConv::str(stat.adapter_mode_))
    if (stat.adapter_mode_ == AdapterMode::kBypass) {
      // Bypass mode is handled differently
      opts.backend_size_ = total_size;
      opts.backend_off_ = off;
      Blob blob_wrap((char*)ptr, total_size);
      WriteBlob(bkt.GetName(), blob_wrap, opts, io_status);
      if (!io_status.success_) {
        HILOG(kDebug, "Failed to write blob of size {} to backend",
              opts.backend_size_)
        return 0;
      }
      if (opts.DoSeek() && !is_append) {
        stat.st_ptr_ = off + total_size;
      }
      return total_size;
    }
    Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);

    if (is_append) {
      // Perform append
      const Blob page((const char*)ptr, total_size);
      bkt.Append(page, stat.page_size_, ctx);
    } else {
      // Fragment I/O request into pages
      BlobPlacements mapping;
      auto mapper = MapperFactory::Get(MapperType::kBalancedMapper);
      mapper->map(off, total_size, stat.page_size_, mapping);
      size_t data_offset = 0;

      // Perform a PartialPut for each page
      for (const BlobPlacement &p : mapping) {
        const Blob page((const char*)ptr + data_offset, p.blob_size_);
        std::string blob_name(p.CreateBlobName().str());
        bkt.AsyncPartialPut(blob_name, page, p.blob_off_, ctx);
        data_offset += p.blob_size_;
      }
      if (opts.DoSeek()) {
        stat.st_ptr_ = off + total_size;
      }
    }
    stat.UpdateTime();
    io_status.size_ = total_size;
    UpdateIoStatus(opts, io_status);

    HILOG(kDebug, "The size of file after write: {}",
          GetSize(f, stat))
    return total_size;
  }

  /** base read function */
  template<bool ASYNC>
  size_t BaseRead(File &f, AdapterStat &stat, void *ptr, size_t off,
                  size_t total_size, size_t req_id,
                  std::vector<LPointer<hrunpq::TypedPushTask<GetBlobTask>>> &tasks,
                  IoStatus &io_status, FsIoOptions opts = FsIoOptions()) {
    (void) f;
    hapi::Bucket &bkt = stat.bkt_id_;

    HILOG(kDebug, "Read called for filename: {}"
                  " on offset: {}"
                  " from position: {}"
                  " and size: {}",
          stat.path_, off, stat.st_ptr_, total_size)

    // SEEK_END is not a valid read position
    if (off == std::numeric_limits<size_t>::max()) {
      io_status.size_ = 0;
      UpdateIoStatus(opts, io_status);
      return 0;
    }

    // Ensure the amount being read makes sense
    if (total_size == 0) {
      io_status.size_ = 0;
      UpdateIoStatus(opts, io_status);
      return 0;
    }

    if constexpr (!ASYNC) {
      if (stat.adapter_mode_ == AdapterMode::kBypass) {
        // Bypass mode is handled differently
        opts.backend_size_ = total_size;
        opts.backend_off_ = off;
        Blob blob_wrap((char *) ptr, total_size);
        ReadBlob(bkt.GetName(), blob_wrap, opts, io_status);
        if (!io_status.success_) {
          HILOG(kDebug, "Failed to read blob of size {} from backend",
                opts.backend_size_)
          return 0;
        }
        if (opts.DoSeek()) {
          stat.st_ptr_ = off + total_size;
        }
        return total_size;
      }
    }

    // Fragment I/O request into pages
    BlobPlacements mapping;
    auto mapper = MapperFactory::Get(MapperType::kBalancedMapper);
    mapper->map(off, total_size, stat.page_size_, mapping);
    size_t data_offset = 0;

    // Perform a PartialPut for each page
    Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
    for (const BlobPlacement &p : mapping) {
      Blob page((const char*)ptr + data_offset, p.blob_size_);
      std::string blob_name(p.CreateBlobName().str());
      if constexpr (ASYNC) {
        LPointer<hrunpq::TypedPushTask<GetBlobTask>> task =
            bkt.AsyncPartialGet(blob_name, page, p.blob_off_, ctx);
        tasks.emplace_back(task);
      } else {
        bkt.PartialGet(blob_name, page, p.blob_off_, ctx);
      }
      data_offset += page.size();
      if (page.size() != p.blob_size_) {
        break;
      }
    }
    if (opts.DoSeek()) {
      stat.st_ptr_ = off + data_offset;
    }
    stat.UpdateTime();
    io_status.size_ = data_offset;
    UpdateIoStatus(opts, io_status);
    return data_offset;
  }

  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr,
              size_t off, size_t total_size,
              IoStatus &io_status, FsIoOptions opts = FsIoOptions()) {
    std::vector<LPointer<hrunpq::TypedPushTask<GetBlobTask>>> tasks;
    return BaseRead<false>(f, stat, ptr, off, total_size, 0, tasks, io_status, opts);
  }

  /** write asynchronously */
  FsAsyncTask* AWrite(File &f, AdapterStat &stat, const void *ptr, size_t off,
                      size_t total_size, size_t req_id, IoStatus &io_status,
                      FsIoOptions opts = FsIoOptions()) {
    // Writes are completely async at this time
    FsAsyncTask *fstask = new FsAsyncTask();
    Write(f, stat, ptr, off, total_size, io_status, opts);
    fstask->io_status_ = io_status;
    fstask->opts_ = opts;
    return fstask;
  }

  /** read asynchronously */
  FsAsyncTask* ARead(File &f, AdapterStat &stat, void *ptr, size_t off,
                     size_t total_size, size_t req_id,
                     IoStatus &io_status, FsIoOptions opts = FsIoOptions()) {
    FsAsyncTask *fstask = new FsAsyncTask();
    BaseRead<true>(f, stat, ptr, off, total_size, req_id, fstask->get_tasks_, io_status, opts);
    fstask->io_status_ = io_status;
    fstask->opts_ = opts;
    return fstask;
  }

  /** wait for \a req_id request ID */
  size_t Wait(FsAsyncTask *fstask) {
    for (LPointer<hrunpq::TypedPushTask<PutBlobTask>> &push_task : fstask->put_tasks_) {
      push_task->Wait();
      HRUN_CLIENT->DelTask(push_task);
    }

    // Update I/O status for gets
    size_t get_size = 0;
    for (LPointer<hrunpq::TypedPushTask<GetBlobTask>> &push_task : fstask->get_tasks_) {
      push_task->Wait();
      GetBlobTask *task = push_task->get();
      get_size += task->data_size_;
      HRUN_CLIENT->DelTask(task);
    }
    fstask->io_status_.size_ = get_size;
    UpdateIoStatus(fstask->opts_, fstask->io_status_);
    return 0;
  }

  /** wait for request IDs in \a req_id vector */
  void Wait(std::vector<FsAsyncTask*> &req_ids, std::vector<size_t> &ret) {
    for (auto &req_id : req_ids) {
      ret.emplace_back(Wait(req_id));
    }
  }

  /** seek */
  size_t Seek(File &f, AdapterStat &stat, SeekMode whence, off64_t offset) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    switch (whence) {
      case SeekMode::kSet: {
        stat.st_ptr_ = offset;
        break;
      }
      case SeekMode::kCurrent: {
        if (stat.st_ptr_ != std::numeric_limits<size_t>::max()) {
          stat.st_ptr_ = (off64_t)stat.st_ptr_ + offset;
          offset = stat.st_ptr_;
        } else {
          stat.st_ptr_ = (off64_t)stat.bkt_id_.GetSize() + offset;
          offset = stat.st_ptr_;
        }
        break;
      }
      case SeekMode::kEnd: {
        if (offset == 0) {
          stat.st_ptr_ = std::numeric_limits<size_t>::max();
          offset = stat.bkt_id_.GetSize();
        } else {
          stat.st_ptr_ = (off64_t)stat.bkt_id_.GetSize() + offset;
          offset = stat.st_ptr_;
        }
        break;
      }
      default: {
        HELOG(kError, "Invalid seek mode");
        return -1;
      }
    }
    mdm->Update(f, stat);
    return offset;
  }

  /** file size */
  size_t GetSize(File &f, AdapterStat &stat) {
    (void) stat;
    if (stat.adapter_mode_ != AdapterMode::kBypass) {
      return stat.bkt_id_.GetSize();
    } else {
      return stdfs::file_size(stat.path_);
    }
  }

  /** tell */
  size_t Tell(File &f, AdapterStat &stat) {
    (void) f;
    if (stat.st_ptr_ != std::numeric_limits<size_t>::max()) {
      return stat.st_ptr_;
    } else {
      return stat.bkt_id_.GetSize();
    }
  }

  /** sync */
  int Sync(File &f, AdapterStat &stat) {
    if (HERMES_CLIENT_CONF.flushing_mode_ == FlushingMode::kSync) {
      // NOTE(llogan): only for the unit tests
      // Please don't enable synchronous flushing
      HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
    }
    return 0;
  }

  /** truncate */
  int Truncate(File &f, AdapterStat &stat, size_t new_size) {
    // hapi::Bucket &bkt = stat.bkt_id_;
    // TODO(llogan)
    return 0;
  }

  /** close */
  int Close(File &f, AdapterStat &stat) {
    Sync(f, stat);
    auto mdm = HERMES_FS_METADATA_MANAGER;
    FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void*)&stat);
    HermesClose(f, stat, fs_ctx);
    RealClose(f, stat);
    mdm->Delete(stat.path_, f);
    if (stat.amode_ & MPI_MODE_DELETE_ON_CLOSE) {
      Remove(stat.path_);
    }
    if (HERMES_CLIENT_CONF.flushing_mode_ == FlushingMode::kSync) {
      // NOTE(llogan): only for the unit tests
      // Please don't enable synchronous flushing
      // stat.bkt_id_.Destroy();
    }
    return 0;
  }

  /** remove */
  int Remove(const std::string &pathname) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    int ret = RealRemove(pathname);
    // Destroy the bucket
    std::string canon_path = stdfs::absolute(pathname).string();
    Bucket bkt = HERMES->GetBucket(canon_path);
    bkt.Destroy();
    // Destroy all file descriptors
    std::list<File>* filesp = mdm->Find(pathname);
    if (filesp == nullptr) {
      return ret;
    }
    HILOG(kDebug, "Destroying the file descriptors: {}", pathname);
    std::list<File> files = *filesp;
    for (File &f : files) {
      std::shared_ptr<AdapterStat> stat = mdm->Find(f);
      if (stat == nullptr) { continue; }
      FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void *)&stat);
      HermesClose(f, *stat, fs_ctx);
      RealClose(f, *stat);
      mdm->Delete(stat->path_, f);
      if (stat->adapter_mode_ == AdapterMode::kScratch) {
        ret = 0;
      }
    }
    return ret;
  }

  /**
   * I/O APIs which seek based on the internal AdapterStat st_ptr,
   * instead of taking an offset as input.
   */

 public:
  /** write */
  size_t Write(File &f, AdapterStat &stat, const void *ptr, size_t total_size,
               IoStatus &io_status, FsIoOptions opts) {
    size_t off = stat.st_ptr_;
    return Write(f, stat, ptr, off, total_size, io_status, opts);
  }

  /** read */
  size_t Read(File &f, AdapterStat &stat, void *ptr, size_t total_size,
              IoStatus &io_status, FsIoOptions opts) {
    size_t off = stat.st_ptr_;
    return Read(f, stat, ptr, off, total_size, io_status, opts);
  }

  /** write asynchronously */
  FsAsyncTask* AWrite(File &f, AdapterStat &stat, const void *ptr,
                      size_t total_size, size_t req_id, IoStatus &io_status,
                      FsIoOptions opts) {
    size_t off = stat.st_ptr_;
    return AWrite(f, stat, ptr, off, total_size, req_id, io_status, opts);
  }

  /** read asynchronously */
  FsAsyncTask* ARead(File &f, AdapterStat &stat, void *ptr, size_t total_size,
             size_t req_id,
             IoStatus &io_status, FsIoOptions opts) {
    size_t off = stat.st_ptr_;
    return ARead(f, stat, ptr, off, total_size, req_id, io_status, opts);
  }

  /**
   * Locates the AdapterStat data structure internally, and
   * call the underlying APIs which take AdapterStat as input.
   */

 public:
  /** write */
  size_t Write(File &f, bool &stat_exists, const void *ptr, size_t total_size,
               IoStatus &io_status, FsIoOptions opts = FsIoOptions()) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    return Write(f, *stat, ptr, total_size, io_status, opts);
  }

  /** read */
  size_t Read(File &f, bool &stat_exists, void *ptr, size_t total_size,
              IoStatus &io_status, FsIoOptions opts = FsIoOptions()) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    return Read(f, *stat, ptr, total_size, io_status, opts);
  }

  /** write \a off offset */
  size_t Write(File &f, bool &stat_exists, const void *ptr, size_t off,
               size_t total_size, IoStatus &io_status,
               FsIoOptions opts = FsIoOptions()) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    opts.UnsetSeek();
    return Write(f, *stat, ptr, off, total_size, io_status, opts);
  }

  /** read \a off offset */
  size_t Read(File &f, bool &stat_exists, void *ptr, size_t off,
              size_t total_size, IoStatus &io_status,
              FsIoOptions opts = FsIoOptions()) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    opts.UnsetSeek();
    return Read(f, *stat, ptr, off, total_size, io_status, opts);
  }

  /** write asynchronously */
  FsAsyncTask* AWrite(File &f, bool &stat_exists, const void *ptr,
                      size_t total_size, size_t req_id,
                      std::vector<PutBlobTask*> &tasks,
                      IoStatus &io_status, FsIoOptions opts) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    return AWrite(f, *stat, ptr, total_size, req_id, io_status, opts);
  }

  /** read asynchronously */
  FsAsyncTask* ARead(File &f, bool &stat_exists, void *ptr, size_t total_size,
                     size_t req_id, IoStatus &io_status, FsIoOptions opts) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    return ARead(f, *stat, ptr, total_size, req_id, io_status, opts);
  }

  /** write \a off offset asynchronously */
  FsAsyncTask* AWrite(File &f, bool &stat_exists, const void *ptr, size_t off,
                      size_t total_size, size_t req_id, IoStatus &io_status,
                      FsIoOptions opts) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    opts.UnsetSeek();
    return AWrite(f, *stat, ptr, off, total_size, req_id, io_status, opts);
  }

  /** read \a off offset asynchronously */
  FsAsyncTask* ARead(File &f, bool &stat_exists, void *ptr, size_t off,
                     size_t total_size, size_t req_id, IoStatus &io_status,
                     FsIoOptions opts) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return 0;
    }
    stat_exists = true;
    opts.UnsetSeek();
    return ARead(f, *stat, ptr, off, total_size, req_id, io_status, opts);
  }

  /** seek */
  size_t Seek(File &f, bool &stat_exists, SeekMode whence, size_t offset) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Seek(f, *stat, whence, offset);
  }

  /** file sizes */
  size_t GetSize(File &f, bool &stat_exists) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return GetSize(f, *stat);
  }

  /** tell */
  size_t Tell(File &f, bool &stat_exists) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Tell(f, *stat);
  }

  /** sync */
  int Sync(File &f, bool &stat_exists) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Sync(f, *stat);
  }

  /** truncate */
  int Truncate(File &f, bool &stat_exists, size_t new_size) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Truncate(f, *stat, new_size);
  }

  /** close */
  int Close(File &f, bool &stat_exists) {
    auto mdm = HERMES_FS_METADATA_MANAGER;
    auto stat = mdm->Find(f);
    if (!stat) {
      stat_exists = false;
      return -1;
    }
    stat_exists = true;
    return Close(f, *stat);
  }

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
