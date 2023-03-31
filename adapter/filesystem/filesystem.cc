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
  stat.adapter_mode_ = mdm->GetAdapterMode(path);
  io_client_->RealOpen(f, stat, path);
  if (!f.status_) { return f; }
  Open(stat, f, path);
  return f;
}

void Filesystem::Open(AdapterStat &stat, File &f, const std::string &path) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  Context ctx;
  std::shared_ptr<AdapterStat> exists = mdm->Find(f);
  if (!exists) {
    VLOG(kDebug) << "File not opened before by adapter" << std::endl;
    // Create the new bucket
    stat.path_ = stdfs::weakly_canonical(path).string();
    auto path_shm = hipc::make_uptr<hipc::charbuf>(stat.path_);
    size_t file_size = io_client_->GetSize(*path_shm);
    // The file was opened with TRUNCATION
    if (stat.is_trunc_) {
      // TODO(llogan): Need to add back bucket lock
      stat.bkt_id_ = HERMES->GetBucket(stat.path_, ctx, 0);
      stat.bkt_id_->Clear(true);
    } else {
      stat.bkt_id_ = HERMES->GetBucket(stat.path_, ctx, file_size);
    }
    // Update page size and file size
    // TODO(llogan): can avoid two unordered_map queries here
    stat.page_size_ = mdm->GetAdapterPageSize(path);
    // The file was opened with APPEND
    if (stat.is_append_) {
      stat.st_ptr_ =  stat.bkt_id_->GetSize(true);
    }
    // Allocate internal hermes data
    auto stat_ptr = std::make_shared<AdapterStat>(stat);
    FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void*)stat_ptr.get());
    io_client_->HermesOpen(f, stat, fs_ctx);
    mdm->Create(f, stat_ptr);
  } else {
    VLOG(kDebug) << "File already opened by adapter" << std::endl;
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
 * @param io_ctx which adapter to route I/O request if blob DNE
 * @param opts which adapter to route I/O request if blob DNE
 * @param ctx any additional information
 * */
Status Filesystem::PartialPutOrCreate(std::shared_ptr<hapi::Bucket> bkt,
                                      const std::string &blob_name,
                                      const Blob &blob,
                                      size_t blob_off,
                                      BlobId &blob_id,
                                      IoStatus &status,
                                      const FsIoOptions &opts,
                                      Context &ctx) {
  Blob full_blob;
  if (bkt->ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    VLOG(kDebug) << "Blob existed. Reading from Hermes." << std::endl;
    bkt->Get(blob_id, full_blob, ctx);
  }
  if (blob_off == 0 &&
      blob.size() >= opts.backend_size_ &&
      blob.size() >= full_blob.size()) {
    // Case 2: We're overriding the entire blob
    // Put the entire blob, no need to load from storage
    VLOG(kDebug) << "Putting the entire blob." << std::endl;
    return bkt->Put(blob_name, blob, blob_id, ctx);
  }
  if (full_blob.size() < opts.backend_size_) {
    // Case 3: The blob did not fully exist (need to read from backend)
    // Read blob using adapter
    VLOG(kDebug) << "Blob did not fully exist. Reading blob from backend. "
              << " cur_size: " << full_blob.size()
              << " backend_size: " << opts.backend_size_
              << std::endl;
    full_blob.resize(opts.backend_size_);
    auto name_shm = hipc::make_uptr<hipc::charbuf>(bkt->GetName());
    io_client_->ReadBlob(*name_shm,
                        full_blob, opts, status);
    if (!status.success_) {
      VLOG(kDebug) << "Failed to read blob of size "
                << opts.backend_size_
                << " from backend (PartialPut)";
      // return PARTIAL_PUT_OR_CREATE_OVERFLOW;
    }
  }
  VLOG(kDebug) << "Modifying full_blob at offset: " << blob_off
            << " for total size: " << blob.size() << std::endl;
  // Ensure the blob can hold the update
  full_blob.resize(std::max(full_blob.size(), blob_off + blob.size()));
  // Modify the blob
  memcpy(full_blob.data() + blob_off, blob.data(), blob.size());
  // Re-put the blob
  bkt->Put(blob_name, full_blob, blob_id, ctx);
  VLOG(kDebug) << "Partially put to blob: (" << blob_id.unique_
            << ", " << blob_id.node_id_ << ")" << std::endl;
  return Status();
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, FsIoOptions opts) {
  (void) f;
  std::shared_ptr<hapi::Bucket> &bkt = stat.bkt_id_;
  std::string filename = bkt->GetName();
  size_t backend_size = stat.bkt_id_->GetSize(true);
  VLOG(kDebug) << "Write called for filename: " << filename
            << " on offset: " << off
            << " from position: " << stat.st_ptr_
            << " and size: " << total_size
            << " and current file size: " << backend_size
            << " and adapter mode: " << AdapterModeConv::str(stat.adapter_mode_)
            << std::endl;

  if (stat.adapter_mode_ == AdapterMode::kBypass) {
    // Bypass mode is handled differently
    opts.backend_size_ = total_size;
    opts.backend_off_ = off;
    Blob blob_wrap((char*)ptr, total_size);
    auto name_shm = hipc::make_uptr<hipc::charbuf>(bkt->GetName());
    io_client_->WriteBlob(*name_shm, blob_wrap, opts, io_status);
    if (!io_status.success_) {
      VLOG(kDebug) << "Failed to write blob of size " << opts.backend_size_
                << " to backend (PartialPut)";
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
    const Blob blob_wrap((const char*)ptr + data_offset, p.blob_size_);
    hshm::charbuf blob_name(p.CreateBlobName());
    BlobId blob_id;
    opts.backend_off_ = p.page_ * kPageSize;
    opts.backend_size_ = GetBackendSize(opts.backend_off_,
                                        backend_size,
                                        kPageSize);
    opts.adapter_mode_ = stat.adapter_mode_;
    bkt->TryCreateBlob(blob_name.str(), blob_id, ctx);
    bkt->LockBlob(blob_id, MdLockType::kExternalWrite);
    auto status = PartialPutOrCreate(bkt,
                                     blob_name.str(),
                                     blob_wrap,
                                     p.blob_off_,
                                     blob_id,
                                     io_status,
                                     opts,
                                     ctx);
    if (status.Fail()) {
      data_offset = 0;
      break;
    }
    size_t new_file_size = opts.backend_off_ + blob_wrap.size();
    if (new_file_size > backend_size) {
      bkt->UpdateSize(new_file_size - backend_size,
                      BucketUpdate::kBackend);
      backend_size = new_file_size;
    }
    bkt->UnlockBlob(blob_id, MdLockType::kExternalWrite);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek()) {
    stat.st_ptr_ = off + data_offset;
  }
  stat.UpdateTime();

  VLOG(kDebug) << "The size of file after write: "
            << GetSize(f, stat) << std::endl;

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
 * @param blob_id [out] the blob id corresponding to blob_name
 * @param io_ctx information required to perform I/O to the backend
 * @param opts specific configuration of the I/O to perform
 * @param ctx any additional information
 * */
Status Filesystem::PartialGetOrCreate(std::shared_ptr<hapi::Bucket> bkt,
                                      const std::string &blob_name,
                                      Blob &blob,
                                      size_t blob_off,
                                      size_t blob_size,
                                      BlobId &blob_id,
                                      IoStatus &status,
                                      const FsIoOptions &opts,
                                      Context &ctx) {
  Blob full_blob;
  if (bkt->ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    VLOG(kDebug) << "Blob existed. Reading blob from Hermes."
              << " offset: " << opts.backend_off_
              << " size: " << blob_size
              << std::endl;
    bkt->Get(blob_id, full_blob, ctx);
  }
  if (full_blob.size() < opts.backend_size_) {
    // Case 2: The blob did not exist (or at least not fully)
    // Read blob using adapter
    VLOG(kDebug) << "Blob did not fully exist. Reading blob from backend. "
              << " cur_size: " << full_blob.size()
              << " backend_size: " << opts.backend_size_
              << std::endl;
    full_blob.resize(opts.backend_size_);
    auto name_shm = hipc::make_uptr<hipc::charbuf>(bkt->GetName());
    io_client_->ReadBlob(*name_shm, full_blob, opts, status);
    if (!status.success_) {
      VLOG(kDebug) << "Failed to read blob of size "
                << opts.backend_size_
                << "from backend (PartialCreate)";
      return PARTIAL_GET_OR_CREATE_OVERFLOW;
    }
    bkt->Put(blob_name, full_blob, blob_id, ctx);
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
  std::shared_ptr<hapi::Bucket> &bkt = stat.bkt_id_;
  std::string filename = bkt->GetName();
  VLOG(kDebug) << "Read called for filename: " << filename
            << " on offset: " << off
            << " from position: " << stat.st_ptr_
            << " and size: " << total_size << std::endl;

  if (stat.adapter_mode_ == AdapterMode::kBypass) {
    // Bypass mode is handled differently
    opts.backend_size_ = total_size;
    opts.backend_off_ = off;
    Blob blob_wrap((char*)ptr, total_size);
    auto name_shm = hipc::make_uptr<hipc::charbuf>(bkt->GetName());
    io_client_->ReadBlob(*name_shm, blob_wrap, opts, io_status);
    if (!io_status.success_) {
      VLOG(kDebug) << "Failed to read blob of size " << opts.backend_size_
                << " to backend (PartialPut)";
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
  size_t backend_size = stat.bkt_id_->GetSize(true);

  for (const auto &p : mapping) {
    Blob blob_wrap((const char*)ptr + data_offset, p.blob_size_);
    hshm::charbuf blob_name(p.CreateBlobName());
    BlobId blob_id;
    opts.backend_off_ = p.page_ * kPageSize;
    opts.backend_size_ = GetBackendSize(opts.backend_off_,
                                        backend_size,
                                        kPageSize);
    opts.adapter_mode_ = stat.adapter_mode_;
    bkt->TryCreateBlob(blob_name.str(), blob_id, ctx);
    bkt->LockBlob(blob_id, MdLockType::kExternalRead);
    auto status = PartialGetOrCreate(stat.bkt_id_,
                                     blob_name.str(),
                                     blob_wrap,
                                     p.blob_off_,
                                     p.blob_size_,
                                     blob_id,
                                     io_status,
                                     opts,
                                     ctx);
    if (status.Fail()) {
      data_offset = 0;
      break;
    }
    bkt->UnlockBlob(blob_id, MdLockType::kExternalRead);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek()) {
    stat.st_ptr_ = off + data_offset;
  }
  stat.UpdateTime();

  ret = data_offset;
  return ret;
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                                  size_t off, size_t total_size, size_t req_id,
                                  IoStatus &io_status, FsIoOptions opts) {
  (void) io_status;
  VLOG(kDebug) << "Starting an asynchronous write" << std::endl;
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
    return stat.bkt_id_->GetSize(true);
  } else {
    return stdfs::file_size(stat.path_);
  }
}

off_t Filesystem::Seek(File &f, AdapterStat &stat,
                       SeekMode whence, off_t offset) {
  if (stat.is_append_) {
    VLOG(kDebug)
        << "File pointer not updating as file was opened in append mode."
        << std::endl;
    return -1;
  }
  auto mdm = HERMES_FS_METADATA_MANAGER;
  switch (whence) {
    case SeekMode::kSet: {
      stat.st_ptr_ = offset;
      break;
    }
    case SeekMode::kCurrent: {
      stat.st_ptr_ += offset;
      break;
    }
    case SeekMode::kEnd: {
      stat.st_ptr_ = stat.bkt_id_->GetSize(true) + offset;
      break;
    }
    default: {
      // TODO(llogan): throw not implemented error.
    }
  }
  mdm->Update(f, stat);
  return stat.st_ptr_;
}

off_t Filesystem::Tell(File &f, AdapterStat &stat) {
  (void) f;
  return stat.st_ptr_;
}

/**
 * Flush a blob
 * */
void Filesystem::FlushBlob(std::shared_ptr<hapi::Bucket> &bkt,
                           BlobId blob_id,
                           AdapterMode mode,
                           Context &ctx) {
  VLOG(kDebug) << "Flushing blob" << std::endl;
  if (mode == AdapterMode::kScratch) {
    VLOG(kDebug) << "In scratch mode, ignoring flush" << std::endl;
    return;
  }
  Blob full_blob;
  IoStatus status;
  // Read blob from Hermes
  bkt->Get(blob_id, full_blob, ctx);
  VLOG(kDebug) << "The blob being flushed has size: "
            << full_blob.size() << std::endl;
  std::string blob_name = bkt->GetBlobName(blob_id);
  // Write blob to backend
  FsIoOptions decode_opts = io_client_->DecodeBlobName(blob_name);
  auto name_shm = hipc::make_uptr<hipc::charbuf>(bkt->GetName());
  io_client_->WriteBlob(*name_shm,
                        full_blob,
                        decode_opts,
                        status);
}

/**
 * Flush the entire bucket
 * */
void Filesystem::Flush(std::shared_ptr<hapi::Bucket> &bkt,
                       AdapterMode mode,
                       Context &ctx) {
  std::vector<BlobId> blob_ids = bkt->GetContainedBlobIds();
  if (mode == AdapterMode::kScratch) {
    return;
  }
  VLOG(kDebug) << "Flushing " << blob_ids.size() << " blobs" << std::endl;
  for (BlobId &blob_id : blob_ids) {
    FlushBlob(bkt, blob_id, mode, ctx);
  }
}

int Filesystem::Sync(File &f, AdapterStat &stat) {
  FsIoOptions opts;
  opts.adapter_mode_ = stat.adapter_mode_;
  if (stat.adapter_mode_ == AdapterMode::kDefault) {
    Flush(stat.bkt_id_, stat.adapter_mode_, stat.bkt_id_->GetContext());
  }
  return 0;
}

int Filesystem::Truncate(File &f, AdapterStat &stat, size_t new_size) {
  // std::shared_ptr<hapi::Bucket> &bkt = stat.bkt_id_;
  // TODO(llogan)
  return 0;
}

int Filesystem::Close(File &f, AdapterStat &stat, bool destroy) {
  Sync(f, stat);
  if (destroy) {
    stat.bkt_id_->Destroy();
  }
  auto mdm = HERMES_FS_METADATA_MANAGER;
  FilesystemIoClientState fs_ctx(&mdm->fs_mdm_, (void*)&stat);
  io_client_->HermesClose(f, stat, fs_ctx);
  io_client_->RealClose(f, stat);
  mdm->Delete(stat.path_, f);
  return 0;
}

int Filesystem::Remove(const std::string &pathname) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  int ret = io_client_->RealRemove(pathname);
  std::list<File>* filesp = mdm->Find(pathname);
  if (filesp == nullptr) {
    return ret;
  }
  std::list<File> files = *filesp;
  for (File &f : files) {
    auto stat = mdm->Find(f);
    if (stat == nullptr) { continue; }
    stat->bkt_id_->Destroy();
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
  off_t off = Tell(f, stat);
  return Write(f, stat, ptr, off, total_size, io_status, opts);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t total_size,
                        IoStatus &io_status, FsIoOptions opts) {
  off_t off = Tell(f, stat);
  return Read(f, stat, ptr, off, total_size, io_status, opts);
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                       size_t total_size, size_t req_id,
                       IoStatus &io_status, FsIoOptions opts) {
  off_t off = Tell(f, stat);
  return AWrite(f, stat, ptr, off, total_size, req_id, io_status, opts);
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                      size_t total_size, size_t req_id,
                      IoStatus &io_status, FsIoOptions opts) {
  off_t off = Tell(f, stat);
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

int Filesystem::Close(File &f, bool &stat_exists, bool destroy) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  auto stat = mdm->Find(f);
  if (!stat) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Close(f, *stat, destroy);
}

}  // namespace hermes::adapter::fs
