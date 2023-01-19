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
#include "metadata_manager.h"
#include "vbucket.h"

#include <fcntl.h>
#include <experimental/filesystem>

namespace stdfs = std::experimental::filesystem;

namespace hermes::adapter::fs {

File Filesystem::Open(AdapterStat &stat, const std::string &path) {
  File f = _RealOpen(stat, path);
  if (!f.status_) { return f; }
  Open(stat, f, path);
  return f;
}

void Filesystem::Open(AdapterStat &stat, File &f, const std::string &path) {
  _InitFile(f);
  auto mdm = HERMES_FILESYSTEM_ADAPTER_METADTA_MANAGER;
  stat.bkt_id_ = HERMES->GetBucket(path);
  LOG(INFO) << "File not opened before by adapter" << std::endl;
  _OpenInitStats(f, stat);
  mdm->Create(f, stat);
}

void Filesystem::_PutWithFallback(AdapterStat &stat,
                                  const std::string &blob_name,
                                  const std::string &filename,
                                  hapi::Blob blob,
                                  size_t offset,
                                  IoStatus &io_status, IoOptions &opts) {
  hapi::Context ctx;
  hermes::BlobId blob_id;
  hapi::Status put_status = stat.bkt_id_->Put(blob_name, blob, blob_id, ctx);
  if (put_status.Fail()) {
    if (opts.with_fallback_) {
      LOG(WARNING) << "Failed to Put Blob " << blob_name << " to Bucket "
                   << filename << ". Falling back to posix I/O." << std::endl;
      _RealWrite(filename, offset, size, data, io_status, opts);
    }
  }
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size,
                         IoStatus &io_status, IoOptions opts) {
  (void) f;
  std::shared_ptr<hapi::Bucket> &bkt = stat.st_bkid;
  std::string filename = bkt->GetName();
  LOG(INFO) << "Write called for filename: " << filename << " on offset: "
            << off << " and size: " << total_size << std::endl;

  size_t ret;
  auto mdm = Singleton<MetadataManager>::GetInstance();
  BlobPlacements mapping;
  auto mapper = MapperFactory().Get(kMapperType);
  mapper->map(off, total_size, mapping);
  size_t data_offset = 0;

  for (const auto &p : mapping) {
    BlobPlacementIter wi(f, stat, filename, p, bkt, io_status, opts);
    wi.blob_name_ = wi.p_.CreateBlobName();
    wi.blob_exists_ = wi.bkt_->ContainsBlob(wi.blob_name_);
    wi.blob_start_ = p.page_ * kPageSize;
    wi.mem_ptr_ = (u8 *)ptr + data_offset;
    data_offset += p.blob_size_;
  }
  off_t f_offset = off + data_offset;
  if (opts.seek_) { stat.st_ptr = f_offset; }
  stat.st_size = std::max(stat.st_size, static_cast<size_t>(f_offset));

  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_mtim = ts;
  stat.st_ctim = ts;

  ret = data_offset;
  _IoStats(data_offset, io_status, opts);
  return ret;
}

HermesRequest* Filesystem::AWrite(File &f, AdapterStat &stat, const void *ptr,
                       size_t off, size_t total_size, size_t req_id,
                       IoStatus &io_status, IoOptions opts) {
  (void) io_status;
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
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->request_map.emplace(req_id, hreq);
  return hreq;
}

HermesRequest* Filesystem::ARead(File &f, AdapterStat &stat, void *ptr,
                      size_t off, size_t total_size, size_t req_id,
                      IoStatus &io_status, IoOptions opts) {
  (void) io_status;
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
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->request_map.emplace(req_id, hreq);
  return hreq;
}

size_t Filesystem::Wait(uint64_t req_id) {
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
  if (stat.is_append_) {
    LOG(INFO)
        << "File pointer not updating as file was opened in append mode."
        << std::endl;
    return -1;
  }
  auto mdm = Singleton<MetadataManager>::GetInstance();
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
      stat.st_ptr_ = stat.st_size + offset;
      break;
    }
    default: {
      // TODO(hari): throw not implemented error.
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
