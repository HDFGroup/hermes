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
#include "constants.h"
#include "singleton.h"
#include "mapper/mapper_factory.h"
#include "interceptor.h"
#include "metadata_manager.h"
#include "vbucket.h"
#include "adapter_utils.cc"

#include <fcntl.h>
#include <experimental/filesystem>

namespace stdfs = std::experimental::filesystem;

namespace hermes::adapter::fs {

static bool IsAsyncFlush(const std::string &path_str) {
  bool result = (INTERCEPTOR_LIST->Persists(path_str) &&
                 global_flushing_mode == FlushingMode::kAsynchronous);
  return result;
}

File Filesystem::Open(AdapterStat &stat, const std::string &path) {
  std::string path_str = WeaklyCanonical(path).string();
  File f = _RealOpen(stat, path);
  if (!f.status_) { return f; }
  Open(stat, f, path);
  return f;
}

void Filesystem::Open(AdapterStat &stat, File &f, const std::string &path) {
  std::string path_str = WeaklyCanonical(path).string();
  _InitFile(f);
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(f);
  if (!existing.second) {
    LOG(INFO) << "File not opened before by adapter" << std::endl;
    stat.ref_count = 1;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_mtim = ts;
    stat.st_ctim = ts;

    /* FIXME(hari) check if this initialization is correct. */
    mdm->InitializeHermes();

    bool bucket_exists = mdm->GetHermes()->BucketExists(path_str);
    // TODO(hari) how to pass to hermes to make a private bucket
    stat.st_bkid =
        std::make_shared<hapi::Bucket>(path_str, mdm->GetHermes());

    if (IsAsyncFlush(path_str)) {
      stat.st_vbkt =
          std::make_shared<hapi::VBucket>(path_str, mdm->GetHermes());
      auto offset_map = std::unordered_map<std::string, u64>();
      stat.st_persist =
          std::make_shared<hapi::PersistTrait>(path_str, offset_map, false);
      stat.st_vbkt->Attach(stat.st_persist.get());
    }

    _OpenInitStats(f, stat, bucket_exists);
    LOG(INFO) << "fd: "<< f.fd_
              << " has size: " << stat.st_size << std::endl;

    mdm->Create(f, stat);
  } else {
    LOG(INFO) << "File opened before by adapter" << std::endl;
    existing.first.ref_count++;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    existing.first.st_atim = ts;
    existing.first.st_ctim = ts;
    mdm->Update(f, existing.first);
  }
}

void Filesystem::_PutWithFallback(AdapterStat &stat,
                                  const std::string &blob_name,
                                  const std::string &filename,
                                  u8 *data, size_t size,
                                  size_t offset,
                                  IoStatus &io_status, IoOptions &opts) {
  hapi::Context ctx;
  const char *hermes_write_only = getenv(kHermesWriteOnlyVar);
  if (hermes_write_only && hermes_write_only[0] == '1') {
    // Custom DPE for write-only apps like VPIC
    ctx.rr_retry = true;
    ctx.disable_swap = true;
  }
  hapi::Status put_status = stat.st_bkid->Put(blob_name, data, size, ctx);
  if (put_status.Failed()) {
    if (opts.with_fallback_) {
      LOG(WARNING) << "Failed to Put Blob " << blob_name << " to Bucket "
                   << filename << ". Falling back to posix I/O." << std::endl;
      _RealWrite(filename, offset, size, data, io_status, opts);
    }
  } else {
    stat.st_blobs.emplace(blob_name);
  }
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, IoOptions opts) {
  (void) f;
  std::shared_ptr<hapi::Bucket> &bkt = stat.st_bkid;
  std::string filename = bkt->GetName();
  LOG(INFO) << "Write called for filename: " << filename << " on offset: "
            << stat.st_ptr << " and size: " << total_size << std::endl;

  size_t ret;
  auto mdm = Singleton<MetadataManager>::GetInstance();
  BlobPlacements mapping;
  auto mapper = MapperFactory().Get(kMapperType);
  mapper->map(off, total_size, mapping);
  size_t data_offset = 0;
  LOG(INFO) << "Mapping for write has " << mapping.size() << " mappings.\n";

  for (const auto &p : mapping) {
    BlobPlacementIter wi(f, stat, filename, p, bkt, io_status, opts);
    wi.blob_name_ = wi.p_.CreateBlobName();
    wi.blob_exists_ = wi.bkt_->ContainsBlob(wi.blob_name_);
    wi.blob_start_ = p.page_ * kPageSize;
    wi.mem_ptr_ = (u8 *)ptr + data_offset;
    if (p.blob_size_ != kPageSize && opts.coordinate_) {
      _CoordinatedPut(wi);
    } else {
      _UncoordinatedPut(wi);
    }
    data_offset += p.blob_size_;
  }
  off_t f_offset = off + data_offset;
  if (opts.seek_) { stat.st_ptr = f_offset; }
  stat.st_size = std::max(stat.st_size, f_offset);

  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_mtim = ts;
  stat.st_ctim = ts;

  mdm->Update(f, stat);
  ret = data_offset;
  _IoStats(data_offset, io_status, opts);
  return ret;
}

void Filesystem::_CoordinatedPut(BlobPlacementIter &wi) {
  hapi::Context ctx;
  LOG(INFO) << "Starting coordinate PUT"
            << " (blob: " << wi.p_.page_ << ")" << std::endl;

  if (!wi.blob_exists_) {
    u8 c = 0;
    auto status = wi.bkt_->Put(wi.blob_name_, &c, 1, ctx);
    if (status.Failed()) {
      LOG(ERROR) << "Not enough space for coordinated put" << std::endl;
    }
  }

  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto &hermes = mdm->GetHermes();
  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;
  BucketID bucket_id = GetBucketId(context, rpc, wi.filename_.c_str());
  BlobID blob_id = GetBlobId(context, rpc, wi.blob_name_, bucket_id, true);

  wi.blob_exists_ = wi.bkt_->ContainsBlob(wi.blob_name_);
  bool is_locked = LockBlob(context, rpc, blob_id);
  if (is_locked) {
    LOG(INFO) << "Acquire lock for process: " << getpid() << std::endl;
    _UncoordinatedPut(wi);
    UnlockBlob(context, rpc, blob_id);
  } else {
    LOG(FATAL) << "Could not acquire blob lock?" << std::endl;
  }
}

void Filesystem::_UncoordinatedPut(BlobPlacementIter &wi) {
  LOG(INFO) << "Starting uncoordinate PUT"
            << " (blob: " << wi.p_.page_ << ")" << std::endl;
  if (wi.blob_exists_) {
    if (wi.p_.blob_off_ == 0) {
      _WriteToExistingAligned(wi);
    } else {
      _WriteToExistingUnaligned(wi);
    }
  } else {
    if (wi.p_.blob_off_ == 0) {
      _WriteToNewAligned(wi);
    } else {
      _WriteToNewUnaligned(wi);
    }
  }
  if (IsAsyncFlush(wi.filename_)) {
    hapi::Trait *trait = wi.stat_.st_vbkt->GetTrait(hapi::TraitType::PERSIST);
    if (trait) {
      hapi::PersistTrait *persist_trait = (hapi::PersistTrait *)trait;
      persist_trait->offset_map.emplace(wi.blob_name_, wi.blob_start_);
    }
    wi.stat_.st_vbkt->Link(wi.blob_name_, wi.filename_);
  }
}

void Filesystem::_WriteToNewAligned(BlobPlacementIter &wi) {
  LOG(INFO) << "Create new blob (aligned)"
            << " offset: " << wi.p_.blob_off_
            << " size: " << wi.p_.blob_size_ << std::endl;
  _PutWithFallback(wi.stat_, wi.blob_name_, wi.filename_, wi.mem_ptr_,
                   wi.p_.blob_size_, wi.p_.bucket_off_,
                   wi.io_status_, wi.opts_);
}

void Filesystem::_WriteToNewUnaligned(BlobPlacementIter &wi) {
  LOG(INFO) << "Create new blob (unaligned)"
      << " offset: " << wi.p_.blob_off_
      << " size: " << wi.p_.blob_size_ << std::endl;
  hapi::Blob final_data(wi.p_.blob_off_ + wi.p_.blob_size_);
  Read(wi.f_, wi.stat_, final_data.data(), wi.blob_start_,
       wi.p_.blob_off_, wi.io_status_);
  memcpy(final_data.data() + wi.p_.blob_off_, wi.mem_ptr_,
         wi.p_.blob_size_);
  _PutWithFallback(wi.stat_, wi.blob_name_, wi.filename_,
                   final_data.data(), final_data.size(), wi.blob_start_,
                   wi.io_status_, wi.opts_);
}

void Filesystem::_WriteToExistingAligned(BlobPlacementIter &wi) {
  LOG(INFO) << "Modify existing blob (aligned)"
            << " offset: " << wi.p_.blob_off_
            << " size: " << wi.p_.blob_size_ << std::endl;

  hapi::Blob temp(0);
  auto existing_blob_size = wi.bkt_->Get(wi.blob_name_, temp);
  if (wi.p_.blob_size_ >= existing_blob_size) {
    LOG(INFO) << "Overwrite blob " << wi.blob_name_
              << " of size:" << wi.p_.blob_size_ << "." << std::endl;
    _PutWithFallback(wi.stat_, wi.blob_name_, wi.filename_,
                     wi.mem_ptr_, wi.p_.blob_size_, wi.blob_start_,
                     wi.io_status_, wi.opts_);
  } else {
    LOG(INFO) << "Update blob " << wi.blob_name_
              << " of size:" << existing_blob_size << "." << std::endl;
    hapi::Blob existing_data(existing_blob_size);
    wi.bkt_->Get(wi.blob_name_, existing_data);
    memcpy(existing_data.data(), wi.mem_ptr_, wi.p_.blob_size_);
    _PutWithFallback(wi.stat_, wi.blob_name_, wi.filename_,
                     existing_data.data(), existing_data.size(),
                     wi.blob_start_, wi.io_status_, wi.opts_);
  }
}

void Filesystem::_WriteToExistingUnaligned(BlobPlacementIter &wi) {
  LOG(INFO) << "Modify existing blob (unaligned)"
            << " offset: " << wi.p_.blob_off_
            << " size: " << wi.p_.blob_size_ << std::endl;

  auto new_size = wi.p_.blob_off_ + wi.p_.blob_size_;
  hapi::Blob temp(0);
  auto existing_blob_size = wi.bkt_->Get(wi.blob_name_, temp);
  hapi::Blob existing_data(existing_blob_size);
  wi.bkt_->Get(wi.blob_name_, existing_data);
  wi.bkt_->DeleteBlob(wi.blob_name_);
  if (new_size < existing_blob_size) {
    new_size = existing_blob_size;
  }
  hapi::Blob final_data(new_size);

  // [0, existing_data)
  memcpy(final_data.data(), existing_data.data(),
         existing_data.size());

  // [existing_data, blob_off)
  if (existing_data.size() < wi.p_.blob_off_) {
    IoOptions opts = IoOptions::DirectIo(wi.opts_);
    Read(wi.f_, wi.stat_,
         final_data.data() + existing_data.size(),
         wi.blob_start_ + existing_data.size(),
         wi.p_.blob_off_ - existing_data.size(),
         wi.io_status_,
         opts);
  }

  // [blob_off, blob_off + blob_size)
  memcpy(final_data.data() + wi.p_.blob_off_, wi.mem_ptr_,
         wi.p_.blob_size_);

  // [blob_off + blob_size, existing_blob_size)
  if (existing_blob_size > wi.p_.blob_off_ + wi.p_.blob_size_) {
    LOG(INFO) << "Retain last portion of blob as Blob is bigger than the "
                 "update." << std::endl;
    auto off_t = wi.p_.blob_off_ + wi.p_.blob_size_;
    memcpy(final_data.data() + off_t, existing_data.data() + off_t,
           existing_blob_size - off_t);
  }

  // Store updated blob
  _PutWithFallback(wi.stat_, wi.blob_name_, wi.filename_,
                   final_data.data(),
                   final_data.size(),
                   wi.blob_start_,
                   wi.io_status_,
                   wi.opts_);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size,
                        IoStatus &io_status, IoOptions opts) {
  (void) f;
  std::shared_ptr<hapi::Bucket> &bkt = stat.st_bkid;
  LOG(INFO) << "Read called for filename: " << bkt->GetName()
            << " (fd: " << f.fd_ << ")"
            << " on offset: " << off
            << " and size: " << total_size
            << " (stored file size: " << stat.st_size
            << " true file size: " << stdfs::file_size(bkt->GetName())
            << ")" << std::endl;
  if (stat.st_ptr >= stat.st_size)  {
    LOG(INFO) << "The current offset: " << stat.st_ptr <<
        " is larger than file size: " << stat.st_size << std::endl;
    return 0;
  }
  size_t ret;
  BlobPlacements mapping;
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  mapper->map(off, total_size, mapping);

  size_t data_offset = 0;
  auto filename = bkt->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &p : mapping) {
    BlobPlacementIter ri(f, stat, filename, p, bkt, io_status, opts);
    ri.blob_name_ = ri.p_.CreateBlobName();
    ri.blob_exists_ = bkt->ContainsBlob(ri.blob_name_);
    ri.blob_start_ = ri.p_.page_ * kPageSize;
    ri.mem_ptr_ = (u8 *)ptr + data_offset;
    size_t read_size;
    if (ri.blob_exists_) {
      size_t min_blob_size = p.blob_off_ + p.blob_size_;
      auto existing_blob_size = bkt->Get(
          ri.blob_name_, ri.blob_, ri.ctx_);
      ri.blob_.resize(existing_blob_size);
      if (existing_blob_size >= min_blob_size) {
        read_size = _ReadExistingContained(ri);
      } else {
        read_size = _ReadExistingPartial(ri);
      }
    } else {
      read_size = _ReadNew(ri);
    }
    data_offset += read_size;
  }
  if (opts.seek_) { stat.st_ptr += data_offset; }
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_atim = ts;
  stat.st_ctim = ts;
  ret = data_offset;
  mdm->Update(f, stat);
  _IoStats(data_offset, io_status, opts);
  return ret;
}

size_t Filesystem::_ReadExistingContained(BlobPlacementIter &ri) {
  LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
            << ri.blob_name_ << "." << std::endl;
  LOG(INFO) << "Blob have data and need to read from hemes "
               "blob: "
            << ri.blob_name_ << " offset:" << ri.p_.blob_off_
            << " size:" << ri.p_.blob_size_ << "." << std::endl;

  ri.bkt_->Get(ri.blob_name_, ri.blob_, ri.ctx_);
  memcpy(ri.mem_ptr_, ri.blob_.data() + ri.p_.blob_off_, ri.p_.blob_size_);
  return ri.p_.blob_size_;
}

size_t Filesystem::_ReadExistingPartial(BlobPlacementIter &ri) {
  LOG(INFO) << "Blob exists and need to partially read from Hermes from blob: "
            << ri.blob_name_ << "." << std::endl;
  /*if (!stdfs::exists(filename) ||
      stdfs::file_size(filename) < p.bucket_off_ + p.blob_off_ + p.blob_size_) {
    return 0;
  }*/
  size_t min_blob_size = ri.p_.blob_off_ + ri.p_.blob_size_;
  size_t existing_size = ri.blob_.size();
  size_t new_blob_size = std::max(min_blob_size, existing_size);
  size_t bytes_to_read = min_blob_size - existing_size;
  ri.blob_.resize(new_blob_size);

  LOG(INFO) << "Blob does not have data and need to read from original "
               "filename: "
            << ri.filename_ << " offset:" << ri.blob_start_ + existing_size
            << " size:" << bytes_to_read << "."
            << std::endl;

  size_t ret = _RealRead(ri.filename_,
                         ri.blob_start_ + existing_size,
                         bytes_to_read,
                         ri.blob_.data() + existing_size,
                         ri.io_status_,
                         ri.opts_);

  if (ri.opts_.dpe_ != PlacementPolicy::kNone) {
    IoOptions opts(ri.opts_);
    opts.seek_ = false;
    opts.with_fallback_ = false;
    Write(ri.f_, ri.stat_,
          ri.blob_.data() + ri.p_.blob_off_,
          ri.p_.bucket_off_,
          new_blob_size, ri.io_status_, opts);
  }

  if (ret != bytes_to_read) {
    LOG(FATAL) << "Was not able to read all data from the file" << std::endl;
  }

  memcpy(ri.mem_ptr_, ri.blob_.data() + ri.p_.blob_off_, ri.p_.blob_size_);
  return ri.p_.blob_size_;
}

size_t Filesystem::_ReadNew(BlobPlacementIter &ri) {
  LOG(INFO)
      << "Blob does not exists and need to read from original filename: "
      << ri.filename_ << " offset:" << ri.p_.bucket_off_
      << " size:" << ri.p_.blob_size_ << "." << std::endl;

  /*if (!stdfs::exists(filename) ||
      stdfs::file_size(filename) < p.bucket_off_ + p.blob_size_) {
    return 0;
  }*/

  auto new_blob_size = ri.p_.blob_off_ + ri.p_.blob_size_;
  ri.blob_.resize(new_blob_size);
  size_t ret = _RealRead(ri.filename_, ri.blob_start_,
                         new_blob_size, ri.blob_.data(),
                         ri.io_status_, ri.opts_);
  if (ret != new_blob_size) {
    LOG(FATAL) << "Was not able to read full content" << std::endl;
  }

  if (ri.opts_.dpe_ != PlacementPolicy::kNone) {
    LOG(INFO) << "Placing the read blob in the hierarchy" << std::endl;
    IoOptions opts(ri.opts_);
    opts.seek_ = false;
    opts.with_fallback_ = false;
    Write(ri.f_, ri.stat_,
          ri.blob_.data() + ri.p_.blob_off_,
          ri.p_.bucket_off_,
          new_blob_size, ri.io_status_, opts);
  }

  memcpy(ri.mem_ptr_, ri.blob_.data() + ri.p_.blob_off_, ri.p_.blob_size_);
  return ri.p_.blob_size_;
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                       size_t off, size_t total_size, size_t req_id,
                       IoStatus &io_status, IoOptions opts) {
  (void) io_status;
  LOG(INFO) << "Starting an asynchronous write" << std::endl;
  auto pool =
      Singleton<ThreadPool>::GetInstance(kNumThreads);
  HermesRequest *req = new HermesRequest();
  auto lambda =
      [](Filesystem *fs, File &f, AdapterStat &stat, const void *ptr,
         size_t off, size_t total_size, IoStatus &io_status, IoOptions opts) {
    return fs->Write(f, stat, ptr, off, total_size, io_status, opts);
  };
  auto func = std::bind(lambda, this, f, stat, ptr, off,
                        total_size, req->io_status, opts);
  req->return_future = pool->run(func);
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->request_map.emplace(req_id, req);
  return req;
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                      size_t off, size_t total_size, size_t req_id,
                      IoStatus &io_status, IoOptions opts) {
  (void) io_status;
  auto pool =
      Singleton<ThreadPool>::GetInstance(kNumThreads);
  HermesRequest *req = new HermesRequest();
  auto lambda =
      [](Filesystem *fs, File &f, AdapterStat &stat, void *ptr,
         size_t off, size_t total_size, IoStatus &io_status, IoOptions opts) {
        return fs->Read(f, stat, ptr, off, total_size, io_status, opts);
      };
  auto func = std::bind(lambda, this, f, stat,
                        ptr, off, total_size, req->io_status, opts);
  req->return_future = pool->run(func);
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->request_map.emplace(req_id, req);
  return req;
}

size_t Filesystem::Wait(size_t req_id) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
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

off_t Filesystem::Seek(File &f, AdapterStat &stat,
                       SeekMode whence, off_t offset) {
  if (stat.is_append) {
    LOG(INFO)
        << "File pointer not updating as file was opened in append mode."
        << std::endl;
    return -1;
  }
  auto mdm = Singleton<MetadataManager>::GetInstance();
  switch (whence) {
    case SeekMode::kSet: {
      stat.st_ptr = offset;
      break;
    }
    case SeekMode::kCurrent: {
      stat.st_ptr += offset;
      break;
    }
    case SeekMode::kEnd: {
      stat.st_ptr = stat.st_size + offset;
      break;
    }
    default: {
      // TODO(hari): throw not implemented error.
    }
  }
  mdm->Update(f, stat);
  return stat.st_ptr;
}

off_t Filesystem::Tell(File &f, AdapterStat &stat) {
  (void) f;
  return stat.st_ptr;
}

int Filesystem::Sync(File &f, AdapterStat &stat) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  if (stat.ref_count != 1) {
    LOG(INFO) << "File handler is opened by more than one fopen."
              << std::endl;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_ctim = ts;
    mdm->Update(f, stat);
    return 0;
  }

  // Wait for all async requests to complete
  for (auto &req : mdm->request_map) {
    Wait(req.first);
  }

  hapi::Context ctx;
  auto filename = stat.st_bkid->GetName();
  auto persist = INTERCEPTOR_LIST->Persists(filename);
  const auto &blob_names = stat.st_blobs;
  if (blob_names.empty() || !persist) {
    return 0;
  }
  if (IsAsyncFlush(filename)) {
    stat.st_vbkt->WaitForBackgroundFlush();
    return 0;
  }

  LOG(INFO) << "POSIX fsync Adapter flushes " << blob_names.size()
            << " blobs to filename:" << filename << "." << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  hapi::VBucket file_vbucket(filename, mdm->GetHermes(), ctx);
  auto offset_map = std::unordered_map<std::string, hermes::u64>();
  for (const auto &blob_name : blob_names) {
    auto status = file_vbucket.Link(blob_name, filename, ctx);
    if (!status.Failed()) {
      BlobPlacement p;
      p.DecodeBlobName(blob_name);
      offset_map.emplace(blob_name, p.page_ * kPageSize);
    }
  }
  bool flush_synchronously = true;
  hapi::PersistTrait persist_trait(filename, offset_map,
                                   flush_synchronously);
  file_vbucket.Attach(&persist_trait, ctx);
  file_vbucket.Destroy(ctx);
  stat.st_blobs.clear();
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  mdm->Update(f, stat);
  return _RealSync(f);
}

int Filesystem::Close(File &f, AdapterStat &stat, bool destroy) {
  hapi::Context ctx;
  auto mdm = Singleton<MetadataManager>::GetInstance();
  if (stat.ref_count != 1) {
    LOG(INFO) << "File handler is opened by more than one fopen."
              << std::endl;
    stat.ref_count--;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_ctim = ts;
    stat.st_bkid->Release(ctx);
    if (stat.st_vbkt) { stat.st_vbkt->Release(); }
    mdm->Update(f, stat);
    return 0;
  }
  Sync(f, stat);
  auto filename = stat.st_bkid->GetName();
  if (IsAsyncFlush(filename)) {
    stat.st_vbkt->Destroy();
  }
  mdm->Delete(f);
  if (destroy) { stat.st_bkid->Destroy(ctx); }
  mdm->FinalizeHermes();
  return _RealClose(f);
}


/**
 * Variants of Read and Write which do not take an offset as
 * input.
 * */

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t total_size, IoStatus &io_status,
                         IoOptions opts) {
  off_t off = Tell(f, stat);
  return Write(f, stat, ptr, off, total_size, io_status, opts);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t total_size,
                        IoStatus &io_status, IoOptions opts) {
  off_t off = Tell(f, stat);
  return Read(f, stat, ptr, off, total_size, io_status, opts);
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                       size_t total_size, size_t req_id,
                       IoStatus &io_status, IoOptions opts) {
  off_t off = Tell(f, stat);
  return AWrite(f, stat, ptr, off, total_size, req_id, io_status, opts);
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                      size_t total_size, size_t req_id,
                      IoStatus &io_status, IoOptions opts) {
  off_t off = Tell(f, stat);
  return ARead(f, stat, ptr, off, total_size, req_id, io_status, opts);
}


/**
 * Variants of the above functions which retrieve the AdapterStat
 * data structure internally.
 * */

size_t Filesystem::Write(File &f, bool &stat_exists, const void *ptr,
                         size_t total_size,
                         IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Write(f, stat, ptr, total_size, io_status, opts);
}

size_t Filesystem::Read(File &f, bool &stat_exists, void *ptr,
                        size_t total_size,
                        IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Read(f, stat, ptr, total_size, io_status, opts);
}

size_t Filesystem::Write(File &f, bool &stat_exists, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  opts.seek_ = false;
  return Write(f, stat, ptr, off, total_size, io_status, opts);
}

size_t Filesystem::Read(File &f, bool &stat_exists, void *ptr,
                        size_t off, size_t total_size,
                        IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  opts.seek_ = false;
  return Read(f, stat, ptr, off, total_size, io_status, opts);
}

HermesRequest* Filesystem::AWrite(File &f, bool &stat_exists, const void *ptr,
                       size_t total_size, size_t req_id,
                       IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return AWrite(f, stat, ptr, total_size, req_id, io_status, opts);
}

HermesRequest* Filesystem::ARead(File &f, bool &stat_exists, void *ptr,
                      size_t total_size, size_t req_id,
                      IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return ARead(f, stat, ptr, total_size, req_id, io_status, opts);
}

HermesRequest* Filesystem::AWrite(File &f, bool &stat_exists, const void *ptr,
                       size_t off, size_t total_size, size_t req_id,
                       IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  opts.seek_ = false;
  return AWrite(f, stat, ptr, off, total_size, req_id, io_status, opts);
}

HermesRequest* Filesystem::ARead(File &f, bool &stat_exists, void *ptr,
                      size_t off, size_t total_size, size_t req_id,
                      IoStatus &io_status, IoOptions opts) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  opts.seek_ = false;
  return ARead(f, stat, ptr, off, total_size, req_id, io_status, opts);
}

off_t Filesystem::Seek(File &f, bool &stat_exists,
                       SeekMode whence, off_t offset) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Seek(f, stat, whence, offset);
}

off_t Filesystem::Tell(File &f, bool &stat_exists) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Tell(f, stat);
}

int Filesystem::Sync(File &f, bool &stat_exists) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Sync(f, stat);
}

int Filesystem::Close(File &f, bool &stat_exists, bool destroy) {
  auto mdm = Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Close(f, stat, destroy);
}

}  // namespace hermes::adapter::fs
