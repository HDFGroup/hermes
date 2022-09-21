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
  File f = _RealOpen(stat, path);
  if (!f.status_) {
    return f;
  }

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
    stat = existing.first;
    mdm->Update(f, existing.first);
  }
  return f;
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

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t total_size) {
  return Write(f, stat, ptr, stat.st_ptr, total_size, true);
}

size_t Filesystem::Write(File &f, AdapterStat &stat, const void *ptr,
                         size_t off, size_t total_size, bool seek) {
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
    u8 *mem_ptr = (u8 *)ptr + data_offset;

    if (blob_exists) {
      if (p.blob_off_ == 0) {
        _WriteToExistingAligned(f, stat, mem_ptr, bucket_off, bkt,
                                filename, p);
      } else {
        _WriteToExistingUnaligned(f, stat, mem_ptr, bucket_off, bkt,
                                  filename, p);
      }
    } else {
      if (p.blob_off_ == 0) {
        _WriteToNewAligned(f, stat, mem_ptr, bucket_off, bkt,
                           filename, p);
      } else {
        _WriteToNewUnaligned(f, stat, mem_ptr, bucket_off, bkt,
                             filename, p);
      }
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

void Filesystem::_WriteToNewAligned(File &f,
                                    AdapterStat &stat,
                                    u8 *mem_ptr,
                                    size_t bucket_off,
                                    std::shared_ptr<hapi::Bucket> bkt,
                                    const std::string &filename,
                                    const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  LOG(INFO) << "Create blob " << p.blob_name_
            << " of size:" << p.blob_size_ << "." << std::endl;
  if (p.blob_size_ == kPageSize) {
    _PutWithFallback(stat, p.blob_name_, filename, mem_ptr,
                     p.blob_size_, p.bucket_off_);
  } else if (p.blob_off_ == 0) {
    _PutWithFallback(stat, p.blob_name_, filename, mem_ptr,
                     p.blob_size_, bucket_off);
  }
}

void Filesystem::_WriteToNewUnaligned(File &f,
                                      AdapterStat &stat,
                                      u8 *mem_ptr,
                                      size_t bucket_off,
                                      std::shared_ptr<hapi::Bucket> bkt,
                                      const std::string &filename,
                                      const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  hapi::Blob final_data(p.blob_off_ + p.blob_size_);
  Read(f, stat, final_data.data(), bucket_off, p.blob_off_);
  memcpy(final_data.data() + p.blob_off_, mem_ptr,
         p.blob_size_);
  _PutWithFallback(stat, p.blob_name_, filename,
                   final_data.data(), final_data.size(), bucket_off);
}

void Filesystem::_WriteToExistingAligned(File &f,
                                         AdapterStat &stat,
                                         u8 *mem_ptr,
                                         size_t bucket_off,
                                         std::shared_ptr<hapi::Bucket> bkt,
                                         const std::string &filename,
                                         const BlobPlacement &p) {
  (void) f;
  (void) bkt;
  hapi::Blob temp(0);
  auto existing_blob_size = bkt->Get(p.blob_name_, temp);
  if (p.blob_size_ == kPageSize) {
    _PutWithFallback(stat, p.blob_name_, filename, mem_ptr,
                     p.blob_size_, p.bucket_off_);
  }
  if (p.blob_off_ == 0) {
    LOG(INFO) << "Blob offset is 0" << std::endl;
    if (p.blob_size_ >= existing_blob_size) {
      LOG(INFO) << "Overwrite blob " << p.blob_name_
                << " of size:" << p.blob_size_ << "." << std::endl;
      _PutWithFallback(stat, p.blob_name_, filename,
                       mem_ptr, p.blob_size_, bucket_off);
    } else {
      LOG(INFO) << "Update blob " << p.blob_name_
                << " of size:" << existing_blob_size << "." << std::endl;
      hapi::Blob existing_data(existing_blob_size);
      bkt->Get(p.blob_name_, existing_data);
      memcpy(existing_data.data(), mem_ptr, p.blob_size_);
      _PutWithFallback(stat, p.blob_name_, filename,
                       existing_data.data(), existing_data.size(),
                       bucket_off);
    }
  }
}

void Filesystem::_WriteToExistingUnaligned(File &f,
                                           AdapterStat &stat,
                                           u8 *mem_ptr,
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
    Read(f, stat,
         final_data.data() + existing_data_cp_size,
         bucket_off + existing_data_cp_size,
         p.blob_off_ - existing_blob_size);
  }
  memcpy(final_data.data() + p.blob_off_, mem_ptr,
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

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t total_size) {
  return Read(f, stat, ptr, stat.st_ptr, total_size, true);
}

size_t Filesystem::Read(File &f, AdapterStat &stat, void *ptr,
                        size_t off, size_t total_size, bool seek) {
  (void) f;
  LOG(INFO) << "Read called for filename: " << stat.st_bkid->GetName()
            << " (fd: " << f.fd_ << ")"
            << " on offset: " << stat.st_ptr
            << " and size: " << total_size
            << " (stored file size: " << stat.st_size
            << " true file size: " << stdfs::file_size(stat.st_bkid->GetName())
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
  auto filename = stat.st_bkid->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &p : mapping) {
    hapi::Context ctx;
    auto blob_exists = stat.st_bkid->ContainsBlob(p.blob_name_);
    u8 *mem_ptr = (u8 *)ptr + data_offset;
    hapi::Blob read_data(0);
    size_t read_size;
    if (blob_exists) {
      size_t min_blob_size = p.blob_off_ + p.blob_size_;
      auto existing_blob_size =
          stat.st_bkid->Get(p.blob_name_, read_data, ctx);
      read_data.resize(existing_blob_size);
      if (existing_blob_size >= min_blob_size) {
        read_size = _ReadExistingContained(f, stat, ctx, read_data, mem_ptr,
                               filename, p);
      } else {
        read_size = _ReadExistingPartial(f, stat, ctx, read_data, mem_ptr,
                             filename, p);
      }
    } else {
      read_size = _ReadNew(f, stat, ctx, read_data, mem_ptr,
               filename, p);
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

size_t Filesystem::_ReadExistingContained(File &f, AdapterStat &stat,
                                          hapi::Context ctx,
                                          hapi::Blob read_data,
                                          u8 *mem_ptr,
                                          const std::string &filename,
                                          const BlobPlacement &p) {
  (void) f; (void) ctx; (void) filename; (void) stat;
  LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
            << p.blob_name_ << "." << std::endl;
  LOG(INFO) << "Blob have data and need to read from hemes "
               "blob: "
            << p.blob_name_ << " offset:" << p.blob_off_
            << " size:" << p.blob_size_ << "." << std::endl;

  stat.st_bkid->Get(p.blob_name_, read_data, ctx);
  memcpy(mem_ptr,
         read_data.data() + p.blob_off_, p.blob_size_);
  return p.blob_size_;
}

size_t Filesystem::_ReadExistingPartial(File &f, AdapterStat &stat,
                                        hapi::Context ctx,
                                        hapi::Blob read_data,
                                        u8 *mem_ptr,
                                        const std::string &filename,
                                        const BlobPlacement &p) {
  LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
            << p.blob_name_ << "." << std::endl;
  (void) f; (void) ctx; (void) filename; (void) stat;
  /*if (!stdfs::exists(filename) ||
      stdfs::file_size(filename) < p.bucket_off_ + p.blob_off_ + p.blob_size_) {
    return 0;
  }*/
  size_t existing_size = read_data.size();
  size_t partial_size = 0;
  if (existing_size > p.blob_off_) {
    partial_size = existing_size - p.blob_off_;
    memcpy(mem_ptr, read_data.data() + p.blob_off_, partial_size);
    mem_ptr += partial_size;
  }

  LOG(INFO) << "Blob does not have data and need to read from original "
               "filename: "
            << filename << " offset:" << p.bucket_off_ + partial_size
            << " size:" << p.blob_size_ - partial_size << "."
            << std::endl;

  size_t ret = _RealRead(filename,
                   p.bucket_off_ + partial_size,
                   p.blob_size_ - partial_size,
                   mem_ptr);
  return ret + partial_size;
}

size_t Filesystem::_ReadNew(File &f, AdapterStat &stat,
                            hapi::Context ctx,
                            hapi::Blob read_data,
                            u8 *mem_ptr,
                            const std::string &filename,
                            const BlobPlacement &p) {
  (void) f; (void) ctx; (void) filename; (void) stat; (void) read_data;
  LOG(INFO)
      << "Blob does not exists and need to read from original filename: "
      << filename << " offset:" << p.bucket_off_
      << " size:" << p.blob_size_ << "." << std::endl;

  /*if (!stdfs::exists(filename) ||
      stdfs::file_size(filename) < p.bucket_off_ + p.blob_size_) {
    return 0;
  }*/
  size_t ret = _RealRead(filename, p.bucket_off_, p.blob_size_, mem_ptr);
  if (ret != p.blob_size_) {
    LOG(FATAL) << "Was not able to read full content" << std::endl;
  }
  return ret;
}

}