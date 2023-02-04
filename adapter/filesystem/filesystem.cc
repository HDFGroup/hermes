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
#include "filesystem_mdm.h"
#include "vbucket.h"
#include "mapper/mapper_factory.h"

#include <fcntl.h>
#include <filesystem>

namespace stdfs = std::filesystem;

namespace hermes::adapter::fs {

File Filesystem::Open(AdapterStat &stat, const std::string &path) {
  File f;
  io_client_->RealOpen(f, stat, path);
  if (!f.status_) { return f; }
  Open(stat, f, path);
  return f;
}

void Filesystem::Open(AdapterStat &stat, File &f, const std::string &path) {
  auto mdm = HERMES_FS_METADATA_MANAGER;
  IoOptions opts;
  opts.type_ = type_;
  stat.bkt_id_ = HERMES->GetBucket(path, ctx_, opts);
  std::shared_ptr<AdapterStat> exists = mdm->Find(f);
  if (!exists) {
    LOG(INFO) << "File not opened before by adapter" << std::endl;
    // Create the new bucket
    IoOptions opts;
    opts.type_ = type_;
    stat.bkt_id_ = HERMES->GetBucket(path, ctx_, opts);
    // Allocate internal hermes data
    auto stat_ptr = std::make_shared<AdapterStat>(stat);
    FilesystemIoClientObject fs_ctx(&mdm->fs_mdm_, (void*)stat_ptr.get());
    io_client_->HermesOpen(f, stat, fs_ctx);
    mdm->Create(f, stat_ptr);
  } else {
    LOG(INFO) << "File opened by adapter" << std::endl;
    exists->UpdateTime();
  }
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, IoOptions opts) {
  (void) f;
  std::shared_ptr<hapi::Bucket> &bkt = stat.bkt_id_;
  std::string filename = bkt->GetName();
  LOG(INFO) << "Write called for filename: " << filename << " on offset: "
            << off << " and size: " << total_size << std::endl;

  size_t ret;
  BlobPlacements mapping;
  auto mapper = MapperFactory().Get(MapperType::kBalancedMapper);
  mapper->map(off, total_size, mapping);
  size_t data_offset = 0;
  size_t kPageSize = HERMES->client_config_.file_page_size_;

  for (const auto &p : mapping) {
    const Blob blob_wrap((const char*)ptr + data_offset, p.blob_size_);
    lipc::charbuf blob_name(p.CreateBlobName());
    BlobId blob_id;
    opts.type_ = type_;
    opts.backend_off_ = p.page_ * kPageSize;
    opts.backend_size_ = kPageSize;
    bkt->PartialPutOrCreate(blob_name.str(),
                            blob_wrap,
                            p.blob_off_,
                            blob_id,
                            opts,
                            ctx_);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek()) { stat.st_ptr_ = off + data_offset; }
  stat.UpdateTime();

  ret = data_offset;
  return ret;
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size,
                        IoStatus &io_status, IoOptions opts) {
  (void) f;
  std::shared_ptr<hapi::Bucket> &bkt = stat.bkt_id_;
  std::string filename = bkt->GetName();
  LOG(INFO) << "Read called for filename: " << filename << " on offset: "
            << off << " and size: " << total_size << std::endl;

  size_t ret;
  BlobPlacements mapping;
  auto mapper = MapperFactory().Get(MapperType::kBalancedMapper);
  mapper->map(off, total_size, mapping);
  size_t data_offset = 0;
  size_t kPageSize = HERMES->client_config_.file_page_size_;

  for (const auto &p : mapping) {
    Blob blob_wrap((const char*)ptr + data_offset, p.blob_size_);
    lipc::charbuf blob_name(p.CreateBlobName());
    BlobId blob_id;
    opts.backend_off_ = p.page_ * kPageSize;
    opts.backend_size_ = kPageSize;
    opts.type_ = type_;
    bkt->PartialGetOrCreate(blob_name.str(),
                            blob_wrap,
                            p.blob_off_,
                            p.blob_size_,
                            blob_id,
                            opts,
                            ctx_);
    data_offset += p.blob_size_;
  }
  if (opts.DoSeek()) { stat.st_ptr_ = off + data_offset; }
  stat.UpdateTime();

  ret = data_offset;
  return ret;
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                                  size_t off, size_t total_size, size_t req_id,
                                  IoStatus &io_status, IoOptions opts) {
  /*(void) io_status;
  LOG(INFO) << "Starting an asynchronous write" << std::endl;
  auto pool =
      Singleton<ThreadPool>::GetInstance(kNumThreads);
  HermesRequest *hreq = new HermesRequest();
  auto lambda =
      [](Filesystem *fs, File &f, AdapterStat &stat, const void *ptr,
         size_t off, size_t total_size, IoStatus &io_status, IoOptions opts) {
    return fs->Write(f, stat, ptr, off, total_size, io_status, opts);
  };
  auto func = std::bind(lambda, this, f, stat, ptr, off,
                        total_size, hreq->io_status, opts);
  hreq->return_future = pool->run(func);
  auto mdm = HERMES_FS_METADATA_MANAGER;
  mdm->request_map.emplace(req_id, hreq);
  return hreq;*/
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                                 size_t off, size_t total_size, size_t req_id,
                                 IoStatus &io_status, IoOptions opts) {
  /*(void) io_status;
  auto pool =
      Singleton<ThreadPool>::GetInstance(kNumThreads);
  HermesRequest *hreq = new HermesRequest();
  auto lambda =
      [](Filesystem *fs, File &f, AdapterStat &stat, void *ptr,
         size_t off, size_t total_size, IoStatus &io_status, IoOptions opts) {
        return fs->Read(f, stat, ptr, off, total_size, io_status, opts);
      };
  auto func = std::bind(lambda, this, f, stat,
                        ptr, off, total_size, hreq->io_status, opts);
  hreq->return_future = pool->run(func);
  auto mdm = HERMES_FS_METADATA_MANAGER;
  mdm->request_map.emplace(req_id, hreq);
  return hreq;*/
}

size_t Filesystem::Wait(uint64_t req_id) {
  /*auto mdm = HERMES_FS_METADATA_MANAGER;
  auto req_iter = mdm->request_map.find(req_id);
  if (req_iter == mdm->request_map.end()) {
    return 0;
  }
  HermesRequest *req = (*req_iter).second;
  size_t ret = req->return_future.get();
  delete req;
  return ret;*/
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
  IoOptions opts;
  opts.type_ = type_;
  return stat.bkt_id_->GetSize(opts);
}

off_t Filesystem::Seek(File &f, AdapterStat &stat,
                       SeekMode whence, off_t offset) {
  if (stat.is_append_) {
    LOG(INFO)
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
      IoOptions opts;
      opts.type_ = type_;
      stat.st_ptr_ = stat.bkt_id_->GetSize(opts) + offset;
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

int Filesystem::Sync(File &f, AdapterStat &stat) {
}

int Filesystem::Close(File &f, AdapterStat &stat, bool destroy) {
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
                        IoStatus &io_status, IoOptions opts) {
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
                         IoStatus &io_status, IoOptions opts) {
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
                        IoStatus &io_status, IoOptions opts) {
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
                       IoStatus &io_status, IoOptions opts) {
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
                      IoStatus &io_status, IoOptions opts) {
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
                       IoStatus &io_status, IoOptions opts) {
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
                      IoStatus &io_status, IoOptions opts) {
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
