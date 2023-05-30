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
#include "mapper/mapper_factory.h"

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
    // Get or create the bucket
    if (stat.hflags_.Any(HERMES_FS_TRUNC)) {
      // The file was opened with TRUNCATION
      stat.bkt_id_ = HERMES->GetBucket(stat.path_, ctx, 0);
      stat.bkt_id_.Clear();
    } else {
      // The file was opened regularly
      stat.file_size_ = io_client_->GetSize(*path_shm);
      stat.bkt_id_ = HERMES->GetBucket(stat.path_, ctx, stat.file_size_);
    }
    HILOG(kDebug, "File has size: {}", stat.bkt_id_.GetSize());
    // Attach trait to bucket (if not scratch mode)
    if (stat.bkt_id_.DidCreate()) {
      io_client_->Register();
      if (stat.adapter_mode_ != AdapterMode::kScratch) {
        stat.bkt_id_.AttachTrait(io_client_->GetTraitId());
      }
    }
    // Update file position pointer
    if (stat.hflags_.Any(HERMES_FS_APPEND)) {
      stat.st_ptr_ = std::numeric_limits<size_t>::max();
    }
    // Update page size
    stat.page_size_ = mdm->GetAdapterPageSize(path);
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

/**
 * The amount of data to read from the backend if the blob
 * does not exist.
 * */
static inline size_t GetBackendSize(size_t file_off,
                                    size_t file_size,
                                    size_t page_size) {
  if (file_off + page_size <= file_size) {
    // Case 1: The file has more than "PageSize" bytes remaining
    return page_size;
  } else if (file_off < file_size) {
    // Case 2: The file has less than "PageSize" bytes remaining
    return file_size - file_off;
  } else {
    // Case 3: The offset is beyond the size of the backend file
    return 0;
  }
}

/**
 * Put \a blob_name Blob into the bucket. Load the blob from the
 * I/O backend if it does not exist.
 *
 * @param blob_name the semantic name of the blob
 * @param blob the buffer to put final data in
 * @param blob_off the offset within the blob to begin the Put
 * @param page_size the page size of the adapter
 * @param io_ctx which adapter to route I/O request if blob DNE
 * @param opts which adapter to route I/O request if blob DNE
 * @param ctx any additional information
 * */
Status Filesystem::PartialPutOrCreate(hapi::Bucket &bkt,
                                      const std::string &blob_name,
                                      const Blob &blob,
                                      size_t blob_off,
                                      size_t page_size,
                                      BlobId &blob_id,
                                      IoStatus &status,
                                      const FsIoOptions &opts,
                                      Context &ctx) {
  Blob full_blob(page_size);
  if (bkt.ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    HILOG(kDebug, "Blob existed. Reading from Hermes.")
    bkt.Get(blob_id, full_blob, ctx);
  }
  if (blob_off == 0 &&
      blob.size() >= opts.backend_size_ &&
      blob.size() >= full_blob.size()) {
    // Case 2: We're overriding the entire blob
    // Put the entire blob, no need to load from storage
    HILOG(kDebug, "Putting the entire blob.")
    return bkt.Put(blob_name, blob, blob_id, ctx);
  }
  if (full_blob.size() < opts.backend_size_) {
    // Case 3: The blob did not fully exist (need to read from backend)
    // Read blob using adapter
    HILOG(kDebug, "Blob did not fully exist. Reading blob from backend."
          " cur_size: {}"
          " backend_size: {}",
          full_blob.size(), opts.backend_size_)
    full_blob.resize(opts.backend_size_);
    io_client_->ReadBlob(bkt.GetName(),
                        full_blob, opts, status);
    if (!status.success_) {
      HELOG(kFatal, "Failed to read blob from {} (PartialPut)."
            " cur_size: {}"
            " backend_off: {}"
            " backend_size: {}",
            bkt.GetName(), full_blob.size(),
            opts.backend_off_, opts.backend_size_)
      // return PARTIAL_PUT_OR_CREATE_OVERFLOW;
    }
  }
  HILOG(kDebug, "Modifying full_blob at offset: {} for total size: {}",
        blob_off, blob.size())
  // Ensure the blob can hold the update
  full_blob.resize(std::max(full_blob.size(), blob_off + blob.size()));
  // Modify the blob
  memcpy(full_blob.data() + blob_off, blob.data(), blob.size());
  // Re-put the blob
  bkt.Put(blob_name, full_blob, blob_id, ctx);
  HILOG(kDebug, "Partially put to blob: ({}, {})",
        blob_id.unique_, blob_id.node_id_)
  return Status();
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, FsIoOptions opts) {
  (void) f;
  hapi::Bucket &bkt = stat.bkt_id_;
  std::string filename = bkt.GetName();

  // Update the size of the bucket
  size_t orig_off = off;
  stat.bkt_id_.LockBucket(MdLockType::kExternalWrite);
  size_t backend_size = stat.bkt_id_.GetSize();
  if (off == std::numeric_limits<size_t>::max()) {
    off = backend_size;
  }
  size_t new_size = off + total_size;
  if (new_size > backend_size) {
    stat.bkt_id_.SetSize(off + total_size);
  }
  stat.bkt_id_.UnlockBucket(MdLockType::kExternalWrite);

  HILOG(kDebug, "Write called for filename: {}"
        " on offset: {}"
        " from position: {}"
        " and size: {}"
        " and current file size: {}"
        " and adapter mode: {}",
        filename, off, stat.st_ptr_, total_size, backend_size,
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
    if (opts.DoSeek() && orig_off != std::numeric_limits<size_t>::max()) {
      stat.st_ptr_ = off + total_size;
    }
    return total_size;
  }

  size_t ret;
  Context ctx;
  BlobPlacements mapping;
  size_t kPageSize = stat.page_size_;
  size_t data_offset = 0;
  auto mapper = MapperFactory().Get(MapperType::kBalancedMapper);
  mapper->map(off, total_size, kPageSize, mapping);

  for (const auto &p : mapping) {
    const Blob blob_wrap((const char*)ptr + data_offset, p.blob_size_);
    hshm::charbuf blob_name(p.CreateBlobName());
    BlobId blob_id;
    opts.backend_off_ = p.page_ * kPageSize;
    opts.backend_size_ = GetBackendSize(opts.backend_off_,
                                        stat.file_size_,
                                        kPageSize);
    opts.adapter_mode_ = stat.adapter_mode_;
    bkt.TryCreateBlob(blob_name.str(), blob_id, ctx);
    bkt.LockBlob(blob_id, MdLockType::kExternalWrite);
    auto status = PartialPutOrCreate(bkt,
                                     blob_name.str(),
                                     blob_wrap,
                                     p.blob_off_,
                                     kPageSize,
                                     blob_id,
                                     io_status,
                                     opts,
                                     ctx);
    if (status.Fail()) {
      data_offset = 0;
      bkt.UnlockBlob(blob_id, MdLockType::kExternalWrite);
      break;
    }
    bkt.UnlockBlob(blob_id, MdLockType::kExternalWrite);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek() && orig_off != std::numeric_limits<size_t>::max()) {
    stat.st_ptr_ = off + total_size;
  }
  stat.UpdateTime();

  HILOG(kDebug, "The size of file after write: {}",
        GetSize(f, stat))
  ret = data_offset;
  return ret;
}

/**
 * Load \a blob_name Blob from the bucket. Load the blob from the
 * I/O backend if it does not exist.
 *
 * @param blob_name the semantic name of the blob
 * @param blob the buffer to put final data in
 * @param blob_off the offset within the blob to begin the Put
 * @param blob_size the total amount of data to read
 * @param page_size the page size of the adapter
 * @param blob_id [out] the blob id corresponding to blob_name
 * @param io_ctx information required to perform I/O to the backend
 * @param opts specific configuration of the I/O to perform
 * @param ctx any additional information
 * */
Status Filesystem::PartialGetOrCreate(hapi::Bucket &bkt,
                                      const std::string &blob_name,
                                      Blob &blob,
                                      size_t blob_off,
                                      size_t blob_size,
                                      size_t page_size,
                                      BlobId &blob_id,
                                      IoStatus &status,
                                      const FsIoOptions &opts,
                                      Context &ctx) {
  Blob full_blob(page_size);
  if (bkt.ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    HILOG(kDebug, "Blob existed. Reading from Hermes."
          " offset: {}"
          " size: {}",
          opts.backend_off_, blob_size)
    bkt.Get(blob_id, full_blob, ctx);
  }
  if (full_blob.size() < opts.backend_size_) {
    // Case 2: The blob did not exist (or at least not fully)
    // Read blob using adapter
    HILOG(kDebug, "Blob did not fully exist. Reading blob from backend."
          " cur_size: {}"
          " backend_size: {}",
          full_blob.size(), opts.backend_size_)
    full_blob.resize(opts.backend_size_);
    io_client_->ReadBlob(bkt.GetName(), full_blob, opts, status);
    if (!status.success_) {
      HILOG(kDebug, "Failed to read blob from backend (PartialGet)."
            " cur_size: {}"
            " backend_size: {}",
            full_blob.size(), opts.backend_size_)
      return PARTIAL_GET_OR_CREATE_OVERFLOW;
    }
    bkt.Put(blob_name, full_blob, blob_id, ctx);
  }
  // Ensure the blob can hold the update
  if (full_blob.size() < blob_off + blob_size) {
    return PARTIAL_GET_OR_CREATE_OVERFLOW;
  }
  // Modify the blob
  // TODO(llogan): we can avoid a copy here
  blob.resize(blob_size);
  memcpy(blob.data(), full_blob.data() + blob_off, blob.size());
  return Status();
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size,
                        IoStatus &io_status, FsIoOptions opts) {
  (void) f;
  hapi::Bucket &bkt = stat.bkt_id_;
  std::string filename = bkt.GetName();
  size_t file_size = stat.bkt_id_.GetSize();

  HILOG(kDebug, "Read called for filename: {}"
                " on offset: {}"
                " from position: {}"
                " and size: {}",
        filename, off, stat.st_ptr_, total_size)

  // SEEK_END is not a valid read position
  if (off == std::numeric_limits<size_t>::max()) {
    return 0;
  }

  // Ensure the amount being read makes sense
  if (off + total_size > file_size) {
    total_size = file_size - off;
  }
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

  size_t ret;
  Context ctx;
  BlobPlacements mapping;
  size_t kPageSize = stat.page_size_;
  size_t data_offset = 0;
  auto mapper = MapperFactory().Get(MapperType::kBalancedMapper);
  mapper->map(off, total_size, kPageSize, mapping);

  for (const auto &p : mapping) {
    Blob blob_wrap((const char*)ptr + data_offset, p.blob_size_);
    hshm::charbuf blob_name(p.CreateBlobName());
    BlobId blob_id;
    opts.backend_off_ = p.page_ * kPageSize;
    opts.backend_size_ = GetBackendSize(opts.backend_off_,
                                        stat.file_size_,
                                        kPageSize);
    opts.adapter_mode_ = stat.adapter_mode_;
    bkt.TryCreateBlob(blob_name.str(), blob_id, ctx);
    bkt.LockBlob(blob_id, MdLockType::kExternalRead);
    auto status = PartialGetOrCreate(stat.bkt_id_,
                                     blob_name.str(),
                                     blob_wrap,
                                     p.blob_off_,
                                     p.blob_size_,
                                     kPageSize,
                                     blob_id,
                                     io_status,
                                     opts,
                                     ctx);
    if (status.Fail()) {
      data_offset = 0;
      bkt.UnlockBlob(blob_id, MdLockType::kExternalRead);
      break;
    }
    bkt.UnlockBlob(blob_id, MdLockType::kExternalRead);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek()) {
    stat.st_ptr_ = off + total_size;
  }
  stat.UpdateTime();

  ret = data_offset;
  return ret;
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                                  size_t off, size_t total_size, size_t req_id,
                                  IoStatus &io_status, FsIoOptions opts) {
  (void) io_status;
  HILOG(kDebug, "Starting an asynchronous write",
        opts.backend_size_)
  auto pool = HERMES_FS_THREAD_POOL;
  HermesRequest *hreq = new HermesRequest();
  auto lambda =
      [](Filesystem *fs, File &f, AdapterStat &stat, const void *ptr,
         size_t off, size_t total_size, IoStatus &io_status, FsIoOptions opts) {
    return fs->Write(f, stat, ptr, off, total_size, io_status, opts);
  };
  auto func = std::bind(lambda, this, f, stat, ptr, off,
                        total_size, hreq->io_status, opts);
  hreq->return_future = pool->run(func);
  auto mdm = HERMES_FS_METADATA_MANAGER;
  mdm->request_map.emplace(req_id, hreq);
  return hreq;/**/
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                                 size_t off, size_t total_size, size_t req_id,
                                 IoStatus &io_status, FsIoOptions opts) {
  (void) io_status;
  auto pool = HERMES_FS_THREAD_POOL;
  HermesRequest *hreq = new HermesRequest();
  auto lambda =
      [](Filesystem *fs, File &f, AdapterStat &stat, void *ptr,
         size_t off, size_t total_size, IoStatus &io_status, FsIoOptions opts) {
        return fs->Read(f, stat, ptr, off, total_size, io_status, opts);
      };
  auto func = std::bind(lambda, this, f, stat,
                        ptr, off, total_size, hreq->io_status, opts);
  hreq->return_future = pool->run(func);
  auto mdm = HERMES_FS_METADATA_MANAGER;
  mdm->request_map.emplace(req_id, hreq);
  return hreq;
}

size_t Filesystem::Wait(uint64_t req_id) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto req_iter = mdm->request_map.find(req_id);
  if (req_iter == mdm->request_map.end()) {
    return 0;
  }
  HermesRequest *req = (*req_iter).second;
  size_t ret = req->return_future.get();
  delete req;
  return ret;
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
  if (HERMES->client_config_.flushing_mode_ == FlushingMode::kSync) {
    // NOTE(llogan): only for the unit tests
    // Please don't enable synchronous flushing
    HERMES->Flush();
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
  if (HERMES->client_config_.flushing_mode_ == FlushingMode::kSync) {
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

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                       size_t total_size, size_t req_id,
                       IoStatus &io_status, FsIoOptions opts) {
  off_t off = stat.st_ptr_;
  return AWrite(f, stat, ptr, off, total_size, req_id, io_status, opts);
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
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

HermesRequest* Filesystem::AWrite(File &f, bool &stat_exists, const void *ptr,
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

HermesRequest* Filesystem::ARead(File &f, bool &stat_exists, void *ptr,
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

HermesRequest* Filesystem::AWrite(File &f, bool &stat_exists, const void *ptr,
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

HermesRequest* Filesystem::ARead(File &f, bool &stat_exists, void *ptr,
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
