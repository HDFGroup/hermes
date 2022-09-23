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

#include <fcntl.h>
#include <experimental/filesystem>

namespace stdfs = std::experimental::filesystem;

namespace hermes::adapter::fs {

static bool PersistEagerly(const std::string &path_str) {
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
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
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

    if (PersistEagerly(path_str)) {
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
    stat.ref_count++;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_ctim = ts;
    mdm->Update(f, stat);
  }
}

void Filesystem::_PutWithFallback(AdapterStat &stat,
                                  const std::string &blob_name,
                                  const std::string &filename,
                                  u8 *data, size_t size,
                                  size_t offset) {
  hapi::Context ctx;
  const char *hermes_write_only = getenv(kHermesWriteOnlyVar);

  if (hermes_write_only && hermes_write_only[0] == '1') {
    // Custom DPE for write-only apps like VPIC
    ctx.rr_retry = true;
    ctx.disable_swap = true;
  }

  hapi::Status status = stat.st_bkid->Put(blob_name, data, size, ctx);
  if (status.Failed()) {
    LOG(WARNING) << "Failed to Put Blob " << blob_name << " to Bucket "
                 << filename << ". Falling back to posix I/O." << std::endl;
    _RealWrite(filename, offset, size, data);
  } else {
    stat.st_blobs.emplace(blob_name);
  }
}

off_t Filesystem::Seek(File &f, AdapterStat &stat, int whence, off_t offset) {
  if (stat.st_mode & O_APPEND) {
    LOG(INFO)
        << "File pointer not updating as file was opened in append mode."
        << std::endl;
    return -1;
  }
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  switch (whence) {
    case SEEK_SET: {
      stat.st_ptr = offset;
      break;
    }
    case SEEK_CUR: {
      stat.st_ptr += offset;
      break;
    }
    case SEEK_END: {
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

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t total_size, PlacementPolicy dpe) {
  return Write(f, stat, ptr, stat.st_ptr, total_size, true, dpe);
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size, bool seek,
                         PlacementPolicy dpe) {
  (void) f; (void) dpe;
  std::shared_ptr<hapi::Bucket> &bkt = stat.st_bkid;
  std::string filename = bkt->GetName();
  LOG(INFO) << "Write called for filename: " << filename << " on offset: "
            << stat.st_ptr << " and size: " << total_size << std::endl;

  size_t ret = 0;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  BlobPlacements mapping;
  auto mapper = MapperFactory().Get(kMapperType);
  mapper->map(off, total_size, mapping);
  size_t data_offset = 0;
  LOG(INFO) << "Mapping for write has " << mapping.size() << " mappings.\n";

  for (const auto &p : mapping) {
    BlobPlacementIter write_iter(f, stat, filename, p, bkt);
    bool blob_exists = bkt->ContainsBlob(p.blob_name_);
    long index = std::stol(p.blob_name_) - 1;
    write_iter.blob_start_ = index * kPageSize;
    write_iter.mem_ptr_ = (u8 *)ptr + data_offset;
    if (blob_exists) {
      if (p.blob_off_ == 0) {
        _WriteToExistingAligned(write_iter);
      } else {
        _WriteToExistingUnaligned(write_iter);
      }
    } else {
      if (p.blob_off_ == 0) {
        _WriteToNewAligned(write_iter);
      } else {
        _WriteToNewUnaligned(write_iter);
      }
    }
    if (PersistEagerly(filename)) {
      hapi::Trait *trait = stat.st_vbkt->GetTrait(hapi::TraitType::PERSIST);
      if (trait) {
        hapi::PersistTrait *persist_trait = (hapi::PersistTrait *)trait;
        persist_trait->offset_map.emplace(p.blob_name_, write_iter.blob_start_);
      }
      stat.st_vbkt->Link(p.blob_name_, filename);
    }
    data_offset += p.blob_size_;
  }
  off_t f_offset = off + data_offset;
  if (seek) { stat.st_ptr = f_offset; }
  stat.st_size = std::max(stat.st_size, f_offset);

  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_mtim = ts;
  stat.st_ctim = ts;
  mdm->Update(f, stat);
  ret = data_offset;
  return ret;
}

void Filesystem::_WriteToNewAligned(BlobPlacementIter &wi) {
  LOG(INFO) << "Create blob " << wi.p_.blob_name_
            << " of size:" << wi.p_.blob_size_ << "." << std::endl;
  if (wi.p_.blob_size_ == kPageSize) {
    _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_, wi.mem_ptr_,
                     wi.p_.blob_size_, wi.p_.bucket_off_);
  } else if (wi.p_.blob_off_ == 0) {
    _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_, wi.mem_ptr_,
                     wi.p_.blob_size_, wi.blob_start_);
  }
}

void Filesystem::_WriteToNewUnaligned(BlobPlacementIter &wi) {
  hapi::Blob final_data(wi.p_.blob_off_ + wi.p_.blob_size_);
  Read(wi.f_, wi.stat_, final_data.data(), wi.blob_start_, wi.p_.blob_off_);
  memcpy(final_data.data() + wi.p_.blob_off_, wi.mem_ptr_,
         wi.p_.blob_size_);
  _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_,
                   final_data.data(), final_data.size(), wi.blob_start_);
}

void Filesystem::_WriteToExistingAligned(BlobPlacementIter &wi) {
  hapi::Blob temp(0);
  auto existing_blob_size = wi.bkt_->Get(wi.p_.blob_name_, temp);
  if (wi.p_.blob_size_ == kPageSize) {
    _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_, wi.mem_ptr_,
                     wi.p_.blob_size_, wi.p_.bucket_off_);
  }
  if (wi.p_.blob_off_ == 0) {
    LOG(INFO) << "Blob offset is 0" << std::endl;
    if (wi.p_.blob_size_ >= existing_blob_size) {
      LOG(INFO) << "Overwrite blob " << wi.p_.blob_name_
                << " of size:" << wi.p_.blob_size_ << "." << std::endl;
      _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_,
                       wi.mem_ptr_, wi.p_.blob_size_, wi.blob_start_);
    } else {
      LOG(INFO) << "Update blob " << wi.p_.blob_name_
                << " of size:" << existing_blob_size << "." << std::endl;
      hapi::Blob existing_data(existing_blob_size);
      wi.bkt_->Get(wi.p_.blob_name_, existing_data);
      memcpy(existing_data.data(), wi.mem_ptr_, wi.p_.blob_size_);
      _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_,
                       existing_data.data(), existing_data.size(),
                       wi.blob_start_);
    }
  }
}

void Filesystem::_WriteToExistingUnaligned(BlobPlacementIter &wi) {
  auto new_size = wi.p_.blob_off_ + wi.p_.blob_size_;
  hapi::Blob temp(0);
  auto existing_blob_size = wi.bkt_->Get(wi.p_.blob_name_, temp);
  hapi::Blob existing_data(existing_blob_size);
  wi.bkt_->Get(wi.p_.blob_name_, existing_data);
  wi.bkt_->DeleteBlob(wi.p_.blob_name_);
  if (new_size < existing_blob_size) {
    new_size = existing_blob_size;
  }
  hapi::Blob final_data(new_size);
  auto existing_data_cp_size = existing_data.size() >= wi.p_.blob_off_
                                   ? wi.p_.blob_off_ : existing_data.size();
  memcpy(final_data.data(), existing_data.data(), existing_data_cp_size);

  if (existing_blob_size < wi.p_.blob_off_ + 1) {
    Read(wi.f_, wi.stat_,
         final_data.data() + existing_data_cp_size,
         wi.blob_start_ + existing_data_cp_size,
         wi.p_.blob_off_ - existing_blob_size);
  }
  memcpy(final_data.data() + wi.p_.blob_off_, wi.mem_ptr_,
         wi.p_.blob_size_);

  if (wi.p_.blob_off_ + wi.p_.blob_size_ < existing_blob_size) {
    LOG(INFO) << "Retain last portion of blob as Blob is bigger than the "
                 "update." << std::endl;
    auto off_t = wi.p_.blob_off_ + wi.p_.blob_size_;
    memcpy(final_data.data() + off_t, existing_data.data() + off_t,
           existing_blob_size - off_t);
  }
  _PutWithFallback(wi.stat_, wi.p_.blob_name_, wi.filename_,
                   final_data.data(),
                   final_data.size(),
                   wi.blob_start_);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t total_size, PlacementPolicy dpe) {
  return Read(f, stat, ptr, stat.st_ptr, total_size, true, dpe);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size, bool seek,
                        PlacementPolicy dpe) {
  (void) f; (void) dpe;
  std::shared_ptr<hapi::Bucket> &bkt = stat.st_bkid;
  LOG(INFO) << "Read called for filename: " << bkt->GetName()
            << " (fd: " << f.fd_ << ")"
            << " on offset: " << stat.st_ptr
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
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  mapper->map(off, total_size, mapping);

  size_t data_offset = 0;
  auto filename = bkt->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &p : mapping) {
    BlobPlacementIter read_iter(f, stat, filename, p, bkt);
    auto blob_exists = bkt->ContainsBlob(p.blob_name_);
    read_iter.mem_ptr_ = (u8 *)ptr + data_offset;
    size_t read_size;
    if (blob_exists) {
      size_t min_blob_size = p.blob_off_ + p.blob_size_;
      auto existing_blob_size = bkt->Get(
          p.blob_name_, read_iter.blob_, read_iter.ctx_);
      read_iter.blob_.resize(existing_blob_size);
      if (existing_blob_size >= min_blob_size) {
        read_size = _ReadExistingContained(read_iter);
      } else {
        read_size = _ReadExistingPartial(read_iter);
      }
    } else {
      read_size = _ReadNew(read_iter);
    }
    data_offset += read_size;
  }
  if (seek) { stat.st_ptr += data_offset; }
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_atim = ts;
  stat.st_ctim = ts;
  ret = data_offset;
  mdm->Update(f, stat);
  return ret;
}

size_t Filesystem::_ReadExistingContained(BlobPlacementIter &ri) {
  LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
            << ri.p_.blob_name_ << "." << std::endl;
  LOG(INFO) << "Blob have data and need to read from hemes "
               "blob: "
            << ri.p_.blob_name_ << " offset:" << ri.p_.blob_off_
            << " size:" << ri.p_.blob_size_ << "." << std::endl;

  ri.bkt_->Get(ri.p_.blob_name_, ri.blob_, ri.ctx_);
  memcpy(ri.mem_ptr_, ri.blob_.data() + ri.p_.blob_off_, ri.p_.blob_size_);
  return ri.p_.blob_size_;
}

size_t Filesystem::_ReadExistingPartial(BlobPlacementIter &ri) {
  LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
            << ri.p_.blob_name_ << "." << std::endl;
  /*if (!stdfs::exists(filename) ||
      stdfs::file_size(filename) < p.bucket_off_ + p.blob_off_ + p.blob_size_) {
    return 0;
  }*/
  size_t existing_size = ri.blob_.size();
  size_t partial_size = 0;
  if (existing_size > ri.p_.blob_off_) {
    partial_size = existing_size - ri.p_.blob_off_;
    memcpy(ri.mem_ptr_, ri.blob_.data() + ri.p_.blob_off_, partial_size);
    ri.mem_ptr_ += partial_size;
  }

  LOG(INFO) << "Blob does not have data and need to read from original "
               "filename: "
            << ri.filename_ << " offset:" << ri.p_.bucket_off_ + partial_size
            << " size:" << ri.p_.blob_size_ - partial_size << "."
            << std::endl;

  size_t ret = _RealRead(ri.filename_,
                         ri.p_.bucket_off_ + partial_size,
                         ri.p_.blob_size_ - partial_size,
                     ri.mem_ptr_);
  return ret + partial_size;
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
  size_t ret = _RealRead(ri.filename_, ri.p_.bucket_off_,
                         ri.p_.blob_size_, ri.mem_ptr_);
  if (ret != ri.p_.blob_size_) {
    LOG(FATAL) << "Was not able to read full content" << std::endl;
  }
  return ret;
}

int Filesystem::Sync(File &f, AdapterStat &stat) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
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

  hapi::Context ctx;
  auto filename = stat.st_bkid->GetName();
  auto persist = INTERCEPTOR_LIST->Persists(filename);
  const auto &blob_names = stat.st_blobs;
  if (blob_names.empty() || !persist) {
    return 0;
  }
  if (PersistEagerly(filename)) {
    stat.st_vbkt->WaitForBackgroundFlush();
    // NOTE(llogan): This doesn't make sense to me
    return 0;
  }

  LOG(INFO) << "POSIX fsync Adapter flushes " << blob_names.size()
            << " blobs to filename:" << filename << "." << std::endl;
  // NOTE(llogan): not in stdio?
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  hapi::VBucket file_vbucket(filename, mdm->GetHermes(), ctx);
  auto offset_map = std::unordered_map<std::string, hermes::u64>();

  for (const auto &blob_name : blob_names) {
    auto status = file_vbucket.Link(blob_name, filename, ctx);
    if (!status.Failed()) {
      auto page_index = std::stol(blob_name) - 1;
      offset_map.emplace(blob_name, page_index * kPageSize);
    }
  }
  bool flush_synchronously = true;
  hapi::PersistTrait persist_trait(filename, offset_map,
                                   flush_synchronously);
  file_vbucket.Attach(&persist_trait, ctx);
  file_vbucket.Destroy(ctx);
  stat.st_blobs.clear();
  // NOTE(llogan): not in stdio?
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  mdm->Update(f, stat);
  return _RealSync(f);
}

int Filesystem::Close(File &f, AdapterStat &stat, bool destroy) {
  hapi::Context ctx;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  if (stat.ref_count != 1) {
    LOG(INFO) << "File handler is opened by more than one fopen."
              << std::endl;
    stat.ref_count--;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_ctim = ts;
    mdm->Update(f, stat);
    stat.st_bkid->Release(ctx);
    if (stat.st_vbkt) { stat.st_vbkt->Release(); }
    return 0;
  }
  Sync(f, stat);
  auto filename = stat.st_bkid->GetName();
  if (PersistEagerly(filename)) {
    stat.st_vbkt->Destroy();
  }
  mdm->Delete(f);
  if (destroy) { stat.st_bkid->Destroy(ctx); }
  mdm->FinalizeHermes();
  return _RealClose(f);
}


/**
 * Variants of the above functions which retrieve the AdapterStat
 * data structure internally.
 * */

size_t Filesystem::Write(File &f, bool &stat_exists, const void *ptr,
             size_t total_size,
             PlacementPolicy dpe) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Write(f, stat, ptr, total_size, dpe);
}

size_t Filesystem::Read(File &f, bool &stat_exists, void *ptr,
            size_t total_size,
            PlacementPolicy dpe) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Read(f, stat, ptr, total_size, dpe);
}

size_t Filesystem::Write(File &f, bool &stat_exists, const void *ptr,
             size_t off, size_t total_size, bool seek,
             PlacementPolicy dpe) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Write(f, stat, ptr, off, total_size, seek, dpe);
}

size_t Filesystem::Read(File &f, bool &stat_exists, void *ptr,
            size_t off, size_t total_size, bool seek,
            PlacementPolicy dpe) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return 0;
  }
  stat_exists = true;
  return Read(f, stat, ptr, off, total_size, seek, dpe);
}

off_t Filesystem::Seek(File &f, bool &stat_exists, int whence, off_t offset) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Seek(f, stat, whence, offset);
}

off_t Filesystem::Tell(File &f, bool &stat_exists) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Tell(f, stat);
}

int Filesystem::Sync(File &f, bool &stat_exists) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Sync(f, stat);
}

int Filesystem::Close(File &f, bool &stat_exists, bool destroy) {
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto [stat, exists] = mdm->Find(f);
  if (!exists) {
    stat_exists = false;
    return -1;
  }
  stat_exists = true;
  return Close(f, stat, destroy);
}

}  // namespace hermes::adapter::fs
