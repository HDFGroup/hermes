//
// Created by lukemartinlogan on 9/19/22.
//

#include "filesystem.h"
#include "constants.h"
#include "singleton.h"
#include "mapper/mapper_factory.h"
#include "interceptor.h"
#include "metadata_manager.h"

#include <fcntl.h>
#include <experimental/filesystem>

namespace stdfs = std::experimental::filesystem;

namespace hermes::adapter::fs {

File Filesystem::Open(AdapterStat &stat, const std::string &path) {
  std::string path_str = WeaklyCanonical(path).string();
  File ret = _RealOpen(stat, path);
  if (!ret.status_) {
    return ret;
  }

  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(ret);
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

    if (bucket_exists) {
      stat.st_size = stat.st_bkid->GetTotalBlobSize();
    }

    if (stat.flags & O_APPEND) {
      stat.st_ptr = stat.st_size;
    }

    mdm->Create(ret, stat);
  } else {
    LOG(INFO) << "File opened before by adapter" << std::endl;
    existing.first.ref_count++;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    existing.first.st_atim = ts;
    existing.first.st_ctim = ts;
    mdm->Update(ret, existing.first);
  }
  return ret;
}

void Filesystem::_PutWithFallback(AdapterStat &stat, const std::string &blob_name,
                          const std::string &filename, u8 *data, size_t size,
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

size_t Filesystem::Write(AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size, File &f) {
  std::shared_ptr<hapi::Bucket> bkt = stat.st_bkid;
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
    bool blob_exists = bkt->ContainsBlob(p.blob_name_);
    long index = std::stol(p.blob_name_) - 1;
    size_t bucket_off = index * kPageSize;
    u8 *put_data_ptr = (u8 *)ptr + data_offset;

    if (blob_exists) {
      if (p.blob_off_ == 0) {
        _WriteToExistingAligned(f, stat, put_data_ptr, bucket_off, bkt,
                                filename, p);
      } else {
        _WriteToExistingUnaligned(f, stat, put_data_ptr, bucket_off, bkt,
                                  filename, p);
      }
    } else {
      if (p.blob_off_ == 0) {
        _WriteToNewAligned(f, stat, put_data_ptr, bucket_off, bkt,
                           filename, p);
      } else {
        _WriteToNewUnaligned(f, stat, put_data_ptr, bucket_off, bkt,
                             filename, p);
      }
    }

    data_offset += p.blob_size_;
  }
  stat.st_ptr += data_offset;
  stat.st_size = stat.st_size >= stat.st_ptr ? stat.st_size : stat.st_ptr;

  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_mtim = ts;
  stat.st_ctim = ts;
  mdm->Update(f, stat);
  ret = data_offset;
  return ret;
}

void Filesystem::_WriteToNewAligned(File &f,
                                    AdapterStat &stat,
                                    u8 *put_data_ptr,
                                    size_t bucket_off,
                                    std::shared_ptr<hapi::Bucket> bkt,
                                    const std::string &filename,
                                    const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  LOG(INFO) << "Create blob " << p.blob_name_
            << " of size:" << p.blob_size_ << "." << std::endl;
  if (p.blob_size_ == kPageSize) {
    _PutWithFallback(stat, p.blob_name_, filename, put_data_ptr,
                     p.blob_size_, p.bucket_off_);
  } else if (p.blob_off_ == 0) {
    _PutWithFallback(stat, p.blob_name_, filename, put_data_ptr,
                     p.blob_size_, bucket_off);
  }
}

void Filesystem::_WriteToNewUnaligned(File &f,
                                      AdapterStat &stat,
                                      u8 *put_data_ptr,
                                      size_t bucket_off,
                                      std::shared_ptr<hapi::Bucket> bkt,
                                      const std::string &filename,
                                      const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  hapi::Blob final_data(p.blob_off_ + p.blob_size_);
  Read(stat, final_data.data(), bucket_off, p.blob_off_, f);
  memcpy(final_data.data() + p.blob_off_, put_data_ptr,
         p.blob_size_);
  _PutWithFallback(stat, p.blob_name_, filename,
                   final_data.data(), final_data.size(), bucket_off);
}

void Filesystem::_WriteToExistingAligned(File &f,
                                         AdapterStat &stat,
                                         u8 *put_data_ptr,
                                         size_t bucket_off,
                                         std::shared_ptr<hapi::Bucket> bkt,
                                         const std::string &filename,
                                         const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  hapi::Blob temp(0);
  auto existing_blob_size = bkt->Get(p.blob_name_, temp);
  if (p.blob_size_ == kPageSize) {
    _PutWithFallback(stat, p.blob_name_, filename, put_data_ptr,
                     p.blob_size_, p.bucket_off_);
  }
  if (p.blob_off_ == 0) {
    LOG(INFO) << "Blob offset is 0" << std::endl;
    if (p.blob_size_ >= existing_blob_size) {
      LOG(INFO) << "Overwrite blob " << p.blob_name_
                << " of size:" << p.blob_size_ << "." << std::endl;
      _PutWithFallback(stat, p.blob_name_, filename,
                       put_data_ptr, p.blob_size_, bucket_off);
    } else {
      LOG(INFO) << "Update blob " << p.blob_name_
                << " of size:" << existing_blob_size << "." << std::endl;
      hapi::Blob existing_data(existing_blob_size);
      bkt->Get(p.blob_name_, existing_data);
      memcpy(existing_data.data(), put_data_ptr, p.blob_size_);
      _PutWithFallback(stat, p.blob_name_, filename,
                       existing_data.data(), existing_data.size(),
                       bucket_off);
    }
  }
}

void Filesystem::_WriteToExistingUnaligned(File &f,
                                           AdapterStat &stat,
                                           u8 *put_data_ptr,
                                           size_t bucket_off,
                                           std::shared_ptr<hapi::Bucket> bkt,
                                           const std::string &filename,
                                           const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  auto new_size = p.blob_off_ + p.blob_size_;
  hapi::Blob temp(0);
  auto existing_blob_size = bkt->Get(p.blob_name_, temp);
  hapi::Blob existing_data(existing_blob_size);
  bkt->Get(p.blob_name_, existing_data);
  bkt->DeleteBlob(p.blob_name_);
  if (new_size < existing_blob_size) {
    new_size = existing_blob_size;
  }
  hapi::Blob final_data(new_size);
  auto existing_data_cp_size = existing_data.size() >= p.blob_off_
                                   ? p.blob_off_ : existing_data.size();
  memcpy(final_data.data(), existing_data.data(), existing_data_cp_size);

  if (existing_blob_size < p.blob_off_ + 1) {
    Read(stat,
         final_data.data() + existing_data_cp_size,
         bucket_off + existing_data_cp_size,
         p.blob_off_ - existing_blob_size,
         f);
  }
  memcpy(final_data.data() + p.blob_off_, put_data_ptr,
         p.blob_size_);

  if (p.blob_off_ + p.blob_size_ < existing_blob_size) {
    LOG(INFO) << "Retain last portion of blob as Blob is bigger than the "
                 "update." << std::endl;
    auto off_t = p.blob_off_ + p.blob_size_;
    memcpy(final_data.data() + off_t, existing_data.data() + off_t,
           existing_blob_size - off_t);
  }
  _PutWithFallback(stat, p.blob_name_, filename,
                   final_data.data(),
                   final_data.size(),
                   bucket_off);
}

size_t Filesystem::Read(AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size, File &f) {
  (void) f;
  LOG(INFO) << "Read called for filename: " << stat.st_bkid->GetName()
            << " on offset: " << stat.st_ptr
            << " and size: " << total_size << std::endl;
  if (stat.st_ptr >= stat.st_size) return 0;
  size_t ret;
  BlobPlacements mapping;
  auto mapper = MapperFactory().Get(kMapperType);
  mapper->map(off, total_size, mapping);
  size_t total_read_size = 0;
  auto filename = stat.st_bkid->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &p : mapping) {
    hapi::Context ctx;
    auto blob_exists =
        stat.st_bkid->ContainsBlob(p.blob_name_);
    hapi::Blob read_data(0);
    size_t read_size = 0;

    if (blob_exists) {
      LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
                << p.blob_name_ << "." << std::endl;
      auto exiting_blob_size =
          stat.st_bkid->Get(p.blob_name_, read_data, ctx);

      read_data.resize(exiting_blob_size);
      stat.st_bkid->Get(p.blob_name_, read_data, ctx);
      bool contains_blob = exiting_blob_size > p.blob_off_;
      if (contains_blob) {
        read_size = read_data.size() < p.blob_off_ + p.blob_size_
                        ? exiting_blob_size - p.blob_off_
                        : p.blob_size_;
        LOG(INFO) << "Blob have data and need to read from hemes "
                     "blob: "
                  << p.blob_name_ << " offset:" << p.blob_off_
                  << " size:" << read_size << "." << std::endl;
        memcpy((char *)ptr + total_read_size,
               read_data.data() + p.blob_off_, read_size);
        if (read_size < p.blob_size_) {
          contains_blob = true;
        } else {
          contains_blob = false;
        }
      } else {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << p.bucket_off_
                  << " size:" << p.blob_size_ << "." << std::endl;
        auto file_read_size =
            _RealRead(filename.c_str(), p.bucket_off_, ptr,
                              total_read_size, p.blob_size_);
        read_size += file_read_size;
      }
      if (contains_blob && stdfs::exists(filename) &&
          stdfs::file_size(filename) >= p.bucket_off_ + p.blob_size_) {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << p.bucket_off_ + read_size
                  << " size:" << p.blob_size_ - read_size << "."
                  << std::endl;
        auto new_read_size = _RealRead(
            filename.c_str(), p.bucket_off_, ptr,
            total_read_size + read_size, p.blob_size_ - read_size);
        read_size += new_read_size;
      }
    } else if (stdfs::exists(filename) &&
               stdfs::file_size(filename) >=
                   p.bucket_off_ + p.blob_size_) {
      LOG(INFO)
          << "Blob does not exists and need to read from original filename: "
          << filename << " offset:" << p.bucket_off_
          << " size:" << p.blob_size_ << "." << std::endl;
      read_size = _RealRead(filename.c_str(), p.bucket_off_, ptr,
                                    total_read_size, p.blob_size_);
    }
    if (read_size > 0) {
      total_read_size += read_size;
    }
  }
  stat.st_ptr += total_read_size;
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_atim = ts;
  stat.st_ctim = ts;
  ret = total_read_size;
  return ret;
}

}