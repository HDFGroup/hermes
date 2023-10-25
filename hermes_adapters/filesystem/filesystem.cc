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

#include "filesystem.h"
#include "hermes_shm/util/singleton.h"
#include "filesystem_mdm.h"
#include "hermes_adapters/mapper/mapper_factory.h"
#include "data_stager/factory/stager_factory.h"

#include <fcntl.h>
#include <filesystem>

namespace stdfs = std::filesystem;

namespace hermes::adapter::fs {

File Filesystem::Open(AdapterStat &stat, const std::string &path) {
  File f;
  auto mdm = HERMES_FS_METADATA_MANAGER;
  if (stat.adapter_mode_ == AdapterMode::kNone) {
    stat.adapter_mode_ = mdm->GetAdapterMode(path);
  }
  io_client_->RealOpen(f, stat, path);
  if (!f.status_) {
    return f;
  }
  Open(stat, f, path);
  return f;
}

void Filesystem::Open(AdapterStat &stat, File &f, const std::string &path) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  Context ctx;
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
    // Get or create the bucket
    hshm::charbuf url = hermes::data_stager::BinaryFileStager::BuildFileUrl(
        stat.path_, stat.page_size_);
    if (stat.hflags_.Any(HERMES_FS_TRUNC)) {
      // The file was opened with TRUNCATION
      stat.bkt_id_ = HERMES->GetBucket(url.str(), ctx, 0, HERMES_IS_FILE);
      stat.bkt_id_.Clear();
    } else {
      // The file was opened regularly
      stat.file_size_ = io_client_->GetSize(*path_shm);
      stat.bkt_id_ = HERMES->GetBucket(url.str(), ctx, stat.file_size_, HERMES_IS_FILE);
    }
    HILOG(kDebug, "File has size: {}", stat.bkt_id_.GetSize());
    // Update file position pointer
    if (stat.hflags_.Any(HERMES_FS_APPEND)) {
      stat.st_ptr_ = std::numeric_limits<size_t>::max();
    }
    // Allocate internal hermes data
    auto stat_ptr = std::make_shared<AdapterStat>(stat);
    FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void*)stat_ptr.get());
    io_client_->HermesOpen(f, stat, fs_ctx);
    mdm->Create(f, stat_ptr);
  } else {
    HILOG(kDebug, "File already opened by adapter")
    exists->UpdateTime();
  }
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, FsIoOptions opts) {
  (void) f;
  hapi::Bucket &bkt = stat.bkt_id_;
  std::string filename = bkt.GetName();

  bool is_append = stat.st_ptr_ == std::numeric_limits<size_t>::max();

  HILOG(kDebug, "Write called for filename: {}"
                " on offset: {}"
                " from position: {}"
                " and current file size: {}"
                " and adapter mode: {}",
        filename, off, stat.st_ptr_, total_size,
        AdapterModeConv::str(stat.adapter_mode_))
  if (stat.adapter_mode_ == AdapterMode::kBypass) {
    // Bypass mode is handled differently
    opts.backend_size_ = total_size;
    opts.backend_off_ = off;
    Blob blob_wrap((char*)ptr, total_size);
    io_client_->WriteBlob(bkt.GetName(), blob_wrap, opts, io_status);
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

  // Fragment I/O request into pages
  BlobPlacements mapping;
  auto mapper = MapperFactory::Get(MapperType::kBalancedMapper);
  mapper->map(off, total_size, stat.page_size_, mapping);
  size_t data_offset = 0;

  // Perform a PartialPut for each page
  Context ctx;
  ctx.page_size_ = stat.page_size_;
  ctx.flags_.SetBits(HERMES_IS_FILE);
  for (const BlobPlacement &p : mapping) {
    const Blob page((const char*)ptr + data_offset, p.blob_size_);
    if (!is_append) {
      std::string blob_name(p.CreateBlobName().str());
      bkt.AsyncPartialPut(blob_name, page, p.blob_off_, ctx);
    } else {
      bkt.Append(page, stat.page_size_, ctx);
    }
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek() && !is_append) {
    stat.st_ptr_ = off + total_size;
  }
  stat.UpdateTime();

  HILOG(kDebug, "The size of file after write: {}",
        GetSize(f, stat))
  return data_offset;
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size,
                        IoStatus &io_status, FsIoOptions opts) {
  (void) f;
  hapi::Bucket &bkt = stat.bkt_id_;

  HILOG(kDebug, "Read called for filename: {}"
                " on offset: {}"
                " from position: {}"
                " and size: {}",
        stat.path_, off, stat.st_ptr_, total_size)

  // SEEK_END is not a valid read position
  if (off == std::numeric_limits<size_t>::max()) {
    return 0;
  }

  // Ensure the amount being read makes sense
  if (total_size == 0) {
    return 0;
  }

  if (stat.adapter_mode_ == AdapterMode::kBypass) {
    // Bypass mode is handled differently
    opts.backend_size_ = total_size;
    opts.backend_off_ = off;
    Blob blob_wrap((char*)ptr, total_size);
    io_client_->ReadBlob(bkt.GetName(), blob_wrap, opts, io_status);
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

  // Fragment I/O request into pages
  BlobPlacements mapping;
  auto mapper = MapperFactory::Get(MapperType::kBalancedMapper);
  mapper->map(off, total_size, stat.page_size_, mapping);
  size_t data_offset = 0;

  // Perform a PartialPut for each page
  Context ctx;
  ctx.flags_.SetBits(HERMES_IS_FILE);
  for (const BlobPlacement &p : mapping) {
    Blob page((const char*)ptr + data_offset, p.blob_size_);
    std::string blob_name(p.CreateBlobName().str());
    bkt.PartialGet(blob_name, page, p.blob_off_, ctx);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek()) {
    stat.st_ptr_ = off + total_size;
  }
  stat.UpdateTime();
  return data_offset;
}

Task* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size, size_t req_id,
                         IoStatus &io_status, FsIoOptions opts) {
  HILOG(kDebug, "Starting an asynchronous write",
        opts.backend_size_);
  return nullptr;
}

Task* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size, size_t req_id,
                        IoStatus &io_status, FsIoOptions opts) {
  return nullptr;
}

size_t Filesystem::Wait(uint64_t req_id) {
  return 0;
}

void Filesystem::Wait(std::vector<uint64_t> &req_ids,
                      std::vector<size_t> &ret) {
  for (auto &req_id : req_ids) {
    ret.emplace_back(Wait(req_id));
  }
}

size_t Filesystem::GetSize(File &f, AdapterStat &stat) {
  (void) stat;
  if (stat.adapter_mode_ != AdapterMode::kBypass) {
    return stat.bkt_id_.GetSize();
  } else {
    return stdfs::file_size(stat.path_);
  }
}

off_t Filesystem::Seek(File &f, AdapterStat &stat,
                       SeekMode whence, off64_t offset) {
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

off_t Filesystem::Tell(File &f, AdapterStat &stat) {
  (void) f;
  if (stat.st_ptr_ != std::numeric_limits<size_t>::max()) {
    return stat.st_ptr_;
  } else {
    return stat.bkt_id_.GetSize();
  }
}

int Filesystem::Sync(File &f, AdapterStat &stat) {
  if (HERMES_CLIENT_CONF.flushing_mode_ == FlushingMode::kSync) {
    // NOTE(llogan): only for the unit tests
    // Please don't enable synchronous flushing
  }
  return 0;
}

int Filesystem::Truncate(File &f, AdapterStat &stat, size_t new_size) {
  // hapi::Bucket &bkt = stat.bkt_id_;
  // TODO(llogan)
  return 0;
}

int Filesystem::Close(File &f, AdapterStat &stat) {
  Sync(f, stat);
  auto mdm = HERMES_FS_METADATA_MANAGER;
  FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void*)&stat);
  io_client_->HermesClose(f, stat, fs_ctx);
  io_client_->RealClose(f, stat);
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

int Filesystem::Remove(const std::string &pathname) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  int ret = io_client_->RealRemove(pathname);
  // Destroy the bucket. It's created if it doesn't exist
  auto bkt = HERMES->GetBucket(pathname);
  HILOG(kDebug, "Destroying the bucket: {}", bkt.GetName());
  bkt.Destroy();
  // Destroy all file descriptors
  std::list<File>* filesp = mdm->Find(pathname);
  if (filesp == nullptr) {
    return ret;
  }
  HILOG(kDebug, "Destroying the file descriptors: {}", pathname);
  std::list<File> files = *filesp;
  for (File &f : files) {
    auto stat = mdm->Find(f);
    if (stat == nullptr) { continue; }
    auto mdm = HERMES_FS_METADATA_MANAGER;
    FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void *)&stat);
    io_client_->HermesClose(f, *stat, fs_ctx);
    io_client_->RealClose(f, *stat);
    mdm->Delete(stat->path_, f);
    if (stat->adapter_mode_ == AdapterMode::kScratch) {
      ret = 0;
    }
  }
  return ret;
}


/**
 * Variants of Read and Write which do not take an offset as
 * input.
 * */

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t total_size, IoStatus &io_status,
                         FsIoOptions opts) {
  off_t off = stat.st_ptr_;
  return Write(f, stat, ptr, off, total_size, io_status, opts);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t total_size,
                        IoStatus &io_status, FsIoOptions opts) {
  off_t off = stat.st_ptr_;
  return Read(f, stat, ptr, off, total_size, io_status, opts);
}

Task* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                         size_t total_size, size_t req_id,
                         IoStatus &io_status, FsIoOptions opts) {
  off_t off = stat.st_ptr_;
  return AWrite(f, stat, ptr, off, total_size, req_id, io_status, opts);
}

Task* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                        size_t total_size, size_t req_id,
                        IoStatus &io_status, FsIoOptions opts) {
  off_t off = stat.st_ptr_;
  return ARead(f, stat, ptr, off, total_size, req_id, io_status, opts);
}


/**
 * Variants of the above functions which retrieve the AdapterStat
 * data structure internally.
 * */

size_t Filesystem::Write(File &f, bool &stat_exists, const void *ptr,
                         size_t total_size,
                         IoStatus &io_status, FsIoOptions opts) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Write(f, *stat, ptr, total_size, io_status, opts);
}

size_t Filesystem::Read(File &f, bool &stat_exists, void *ptr,
                        size_t total_size,
                        IoStatus &io_status, FsIoOptions opts) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Read(f, *stat, ptr, total_size, io_status, opts);
}

size_t Filesystem::Write(File &f, bool &stat_exists, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, FsIoOptions opts) {
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

size_t Filesystem::Read(File &f, bool &stat_exists, void *ptr,
                        size_t off, size_t total_size,
                        IoStatus &io_status, FsIoOptions opts) {
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

Task* Filesystem::AWrite(File &f, bool &stat_exists, const void *ptr,
                         size_t total_size, size_t req_id,
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

Task* Filesystem::ARead(File &f, bool &stat_exists, void *ptr,
                        size_t total_size, size_t req_id,
                        IoStatus &io_status, FsIoOptions opts) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return ARead(f, *stat, ptr, total_size, req_id, io_status, opts);
}

Task* Filesystem::AWrite(File &f, bool &stat_exists, const void *ptr,
                         size_t off, size_t total_size, size_t req_id,
                         IoStatus &io_status, FsIoOptions opts) {
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

Task* Filesystem::ARead(File &f, bool &stat_exists, void *ptr,
                        size_t off, size_t total_size, size_t req_id,
                        IoStatus &io_status, FsIoOptions opts) {
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

off_t Filesystem::Seek(File &f, bool &stat_exists,
                       SeekMode whence, off_t offset) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Seek(f, *stat, whence, offset);
}

size_t Filesystem::GetSize(File &f, bool &stat_exists) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return GetSize(f, *stat);
}

off_t Filesystem::Tell(File &f, bool &stat_exists) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Tell(f, *stat);
}

int Filesystem::Sync(File &f, bool &stat_exists) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Sync(f, *stat);
}

int Filesystem::Truncate(File &f, bool &stat_exists, size_t new_size) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Truncate(f, *stat, new_size);
}

int Filesystem::Close(File &f, bool &stat_exists) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Close(f, *stat);
}

}  // namespace hermes::adapter::fs
