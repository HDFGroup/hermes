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

#include <hermes/adapter/stdio.h>

#include <limits.h>
#include <sys/file.h>

#include <hermes/adapter/interceptor.cc>
#include <hermes/adapter/utils.cc>
#include <hermes/adapter/stdio/mapper/balanced_mapper.cc>
#include <hermes/adapter/stdio/metadata_manager.cc>

using hermes::adapter::stdio::AdapterStat;
using hermes::adapter::stdio::FileStruct;
using hermes::adapter::stdio::MapperFactory;
using hermes::adapter::stdio::MetadataManager;
using hermes::adapter::stdio::global_flushing_mode;
using hermes::adapter::WeaklyCanonical;
using hermes::adapter::ReadGap;

using hermes::u8;
using hermes::u64;

namespace hapi = hermes::api;
namespace fs = std::experimental::filesystem;


/**
 * Internal Functions
 */

size_t perform_file_write(const std::string &filename, size_t offset,
                          size_t size, u8 *data_ptr) {
  LOG(INFO) << "Writing to file: " << filename << " offset: " << offset
            << " of size:" << size << "." << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  FILE *fh = fopen(filename.c_str(), "r+");
  size_t write_size = 0;
  if (fh != nullptr) {
    if (fseek(fh, offset, SEEK_SET) == 0) {
      write_size = fwrite(data_ptr, sizeof(char), size, fh);
      if (fclose(fh) != 0) {
        hermes::FailedLibraryCall("fclose");
      }
    } else {
      hermes::FailedLibraryCall("fseek");
    }
  } else {
    hermes::FailedLibraryCall("fopen");
  }
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  return write_size;
}

size_t perform_file_read(const char *filename, size_t file_offset, void *ptr,
                         size_t ptr_offset, size_t size) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << file_offset << " and size: " << size
            << std::endl;
  INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
  FILE *fh = fopen(filename, "r");
  size_t read_size = 0;
  if (fh != nullptr) {
    flock(fileno(fh), LOCK_SH);
    if (fseek(fh, file_offset, SEEK_SET) == 0) {
      read_size = fread((char *)ptr + ptr_offset, sizeof(char), size, fh);
      flock(fileno(fh), LOCK_UN);
      if (fclose(fh) != 0) {
        hermes::FailedLibraryCall("fclose");
      }
    } else {
      hermes::FailedLibraryCall("fseek");
    }
  } else {
    hermes::FailedLibraryCall("fopen");
  }
  INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);

  return read_size;
}

static bool PersistEagerly(const std::string &path_str) {
  bool result = (INTERCEPTOR_LIST->Persists(path_str) &&
                 global_flushing_mode == FlushingMode::kAsynchronous);

  return result;
}

/**
 * MPI
 */
int HERMES_DECL(MPI_Init)(int *argc, char ***argv) {
  MAP_OR_FAIL(MPI_Init);
  int status = real_MPI_Init_(argc, argv);
  if (status == 0) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    mdm->InitializeHermes(true);
    LOG(INFO) << "MPI Init intercepted." << std::endl;
  }
  return status;
}

int HERMES_DECL(MPI_Finalize)(void) {
  LOG(INFO) << "MPI Finalize intercepted." << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  MAP_OR_FAIL(MPI_Finalize);
  int status = real_MPI_Finalize_();
  return status;
}

/**
 * STDIO
 */
FILE *simple_open(FILE *ret, const std::string &user_path, const char *mode) {
  std::string path_str = WeaklyCanonical(user_path).string();

  LOG(INFO) << "Open file for filename " << path_str << " in mode " << mode
            << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();

  if (ret) {
    auto existing = mdm->Find(ret);
    if (!existing.second) {
      LOG(INFO) << "File not opened before by adapter" << std::endl;
      struct stat st;
      int fd = fileno(ret);
      int status = fstat(fd, &st);
      if (status == 0) {
        AdapterStat stat(st);
        stat.ref_count = 1;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        stat.st_atim = ts;
        stat.st_mtim = ts;
        stat.st_ctim = ts;
        if (strcmp(mode, "a") == 0 || strcmp(mode, "a+") == 0) {
          // FIXME(hari): get current size of bucket from Hermes
          stat.st_ptr = stat.st_size;
        }
        // FIXME(hari): check if this initialization is correct.
        mdm->InitializeHermes();
        // TODO(hari): how to pass to hermes to make a private bucket
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

        mdm->Create(ret, stat);
      } else {
        // TODO(hari): @errorhandling invalid fh.
        ret = nullptr;
      }
    } else {
      LOG(INFO) << "File opened before by adapter" << std::endl;
      existing.first.ref_count++;
      struct timespec ts;
      timespec_get(&ts, TIME_UTC);
      existing.first.st_atim = ts;
      existing.first.st_ctim = ts;
      mdm->Update(ret, existing.first);
    }
  }

  return ret;
}

FILE *open_internal(const std::string &path_str, const char *mode) {
  FILE *ret;
  MAP_OR_FAIL(fopen);
  ret = real_fopen_(path_str.c_str(), mode);
  ret = simple_open(ret, path_str.c_str(), mode);
  return ret;
}

FILE *reopen_internal(const std::string &user_path, const char *mode,
                      FILE *stream) {
  FILE *ret;
  MAP_OR_FAIL(freopen);
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  ret = real_freopen_(user_path.c_str(), mode, stream);
  if (!ret) {
    return ret;
  } else {
    std::string path_str = WeaklyCanonical(user_path).string();
    LOG(INFO) << "Reopen file for filename " << path_str << " in mode " << mode
              << std::endl;
    auto existing = mdm->Find(ret);
    if (!existing.second) {
      LOG(INFO) << "File not opened before by adapter" << std::endl;
      return nullptr;
    } else {
      LOG(INFO) << "File opened before by adapter" << std::endl;
      struct timespec ts;
      timespec_get(&ts, TIME_UTC);
      existing.first.st_atim = ts;
      existing.first.st_ctim = ts;
      mdm->Update(ret, existing.first);
    }
  }
  return ret;
}

void PutWithStdioFallback(AdapterStat &stat, const std::string &blob_name,
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
                 << filename << ". Falling back to stdio." << std::endl;
    perform_file_write(filename, offset, size, data);
  } else {
    stat.st_blobs.emplace(blob_name);
  }
}

size_t write_internal(AdapterStat &stat, const void *ptr, size_t total_size,
                      FILE *fp) {
  std::shared_ptr<hapi::Bucket> bkt = stat.st_bkid;
  std::string filename = bkt->GetName();

  LOG(INFO) << "Write called for filename: " << filename << " on offset: "
            << stat.st_ptr << " and size: " << total_size << std::endl;

  size_t ret = 0;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  auto mapping = mapper->map(
      FileStruct(mdm->Convert(fp), stat.st_ptr, total_size));
  size_t data_offset = 0;
  LOG(INFO) << "Mapping for write has " << mapping.size() << " mappings.\n";

  for (const auto &[finfo, hinfo] : mapping) {
    auto index = std::stol(hinfo.blob_name_) - 1;
    size_t offset = index * kPageSize;
    auto blob_exists = bkt->ContainsBlob(hinfo.blob_name_);
    u8 *put_data_ptr = (u8 *)ptr + data_offset;
    size_t put_data_ptr_size = finfo.size_;

    if (!blob_exists || hinfo.size_ == kPageSize) {
      LOG(INFO) << "Create or Overwrite blob " << hinfo.blob_name_
                << " of size:" << hinfo.size_ << "." << std::endl;
      if (hinfo.size_ == kPageSize) {
        PutWithStdioFallback(stat, hinfo.blob_name_, filename, put_data_ptr,
                             put_data_ptr_size, finfo.offset_);
      } else if (hinfo.offset_ == 0) {
        PutWithStdioFallback(stat, hinfo.blob_name_, filename, put_data_ptr,
                             put_data_ptr_size, offset);
      } else {
        hapi::Blob final_data(hinfo.offset_ + hinfo.size_);

        ReadGap(filename, offset, final_data.data(), hinfo.offset_,
                hinfo.offset_);
        memcpy(final_data.data() + hinfo.offset_, put_data_ptr,
               put_data_ptr_size);
        PutWithStdioFallback(stat, hinfo.blob_name_, filename,
                             final_data.data(), final_data.size(), offset);
      }
    } else {
      LOG(INFO) << "Blob " << hinfo.blob_name_
                << " of size:" << hinfo.size_ << " exists." << std::endl;
      hapi::Blob temp(0);
      auto existing_blob_size = bkt->Get(hinfo.blob_name_, temp);
      if (hinfo.offset_ == 0) {
        LOG(INFO) << "Blob offset is 0" << std::endl;
        if (hinfo.size_ >= existing_blob_size) {
          LOG(INFO) << "Overwrite blob " << hinfo.blob_name_
                    << " of size:" << hinfo.size_ << "." << std::endl;
          PutWithStdioFallback(stat, hinfo.blob_name_, filename,
                               put_data_ptr, put_data_ptr_size, offset);
        } else {
          LOG(INFO) << "Update blob " << hinfo.blob_name_
                    << " of size:" << existing_blob_size << "." << std::endl;
          hapi::Blob existing_data(existing_blob_size);
          bkt->Get(hinfo.blob_name_, existing_data);
          memcpy(existing_data.data(), put_data_ptr, put_data_ptr_size);

          PutWithStdioFallback(stat, hinfo.blob_name_, filename,
                               existing_data.data(), existing_data.size(),
                               offset);
        }
      } else {
        LOG(INFO) << "Blob offset: " << hinfo.offset_ << "." << std::endl;
        auto new_size = hinfo.offset_ + hinfo.size_;
        hapi::Blob existing_data(existing_blob_size);
        bkt->Get(hinfo.blob_name_, existing_data);
        bkt->DeleteBlob(hinfo.blob_name_);
        if (new_size < existing_blob_size) {
          new_size = existing_blob_size;
        }
        hapi::Blob final_data(new_size);
        auto existing_data_cp_size = existing_data.size() >= hinfo.offset_
                                         ? hinfo.offset_ : existing_data.size();
        memcpy(final_data.data(), existing_data.data(), existing_data_cp_size);

        if (existing_blob_size < hinfo.offset_ + 1) {
          ReadGap(filename, offset + existing_data_cp_size,
                  final_data.data() + existing_data_cp_size,
                  hinfo.offset_ - existing_blob_size,
                  hinfo.offset_ + hinfo.size_);
        }
        memcpy(final_data.data() + hinfo.offset_, put_data_ptr,
               put_data_ptr_size);

        if (hinfo.offset_ + hinfo.size_ < existing_blob_size) {
          LOG(INFO) << "Retain last portion of blob as Blob is bigger than the "
                       "update." << std::endl;
          auto off_t = hinfo.offset_ + hinfo.size_;
          memcpy(final_data.data() + off_t, existing_data.data() + off_t,
                 existing_blob_size - off_t);
        }

        PutWithStdioFallback(stat, hinfo.blob_name_, filename,
                             final_data.data(), final_data.size(), offset);
      }
    }
    data_offset += finfo.size_;

    if (PersistEagerly(filename)) {
      hapi::Trait *trait = stat.st_vbkt->GetTrait(hapi::TraitType::PERSIST);
      if (trait) {
        hapi::PersistTrait *persist_trait = (hapi::PersistTrait *)trait;
        persist_trait->offset_map.emplace(hinfo.blob_name_, offset);
      }

      stat.st_vbkt->Link(hinfo.blob_name_, filename);
    }
  }
  stat.st_ptr += data_offset;
  stat.st_size = stat.st_size >= stat.st_ptr ? stat.st_size : stat.st_ptr;

  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  stat.st_mtim = ts;
  stat.st_ctim = ts;
  mdm->Update(fp, stat);
  ret = data_offset;

  return ret;
}

size_t read_internal(AdapterStat &stat, void *ptr, size_t total_size,
                     FILE *fp) {
  std::shared_ptr<hapi::Bucket> bkt = stat.st_bkid;
  auto filename = bkt->GetName();
  LOG(INFO) << "Read called for filename: " << filename << " on offset: "
            << stat.st_ptr << " and size: " << total_size << std::endl;
  if (stat.st_ptr >= stat.st_size) {
    return 0;
  }

  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(kMapperType);
  auto mapping = mapper->map(
      FileStruct(mdm->Convert(fp), stat.st_ptr, total_size));
  size_t total_read_size = 0;
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto& [finfo, hinfo] : mapping) {
    auto blob_exists = bkt->ContainsBlob(hinfo.blob_name_);
    hapi::Blob read_data(0);
    size_t read_size = 0;
    if (blob_exists) {
      LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
                << hinfo.blob_name_ << "." << std::endl;
      auto existing_blob_size = bkt->Get(hinfo.blob_name_, read_data);

      read_data.resize(existing_blob_size);
      bkt->Get(hinfo.blob_name_, read_data);
      bool contains_blob = existing_blob_size > hinfo.offset_;
      if (contains_blob) {
        read_size = read_data.size() < hinfo.offset_ + hinfo.size_
                        ? existing_blob_size - hinfo.offset_ : hinfo.size_;
        LOG(INFO) << "Blob have data and need to read from hemes "
                     "blob: "
                  << hinfo.blob_name_ << " offset:" << hinfo.offset_
                  << " size:" << read_size << "." << std::endl;
        memcpy((char *)ptr + total_read_size,
               read_data.data() + hinfo.offset_, read_size);
        if (read_size < hinfo.size_) {
          contains_blob = true;
        } else {
          contains_blob = false;
        }
      } else {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << finfo.offset_
                  << " size:" << finfo.size_ << "." << std::endl;
        auto file_read_size =
            perform_file_read(filename.c_str(), finfo.offset_, ptr,
                              total_read_size, hinfo.size_);
        read_size += file_read_size;
      }
      if (contains_blob && fs::exists(filename) &&
          fs::file_size(filename) >= finfo.offset_ + finfo.size_) {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << finfo.offset_ + read_size
                  << " size:" << hinfo.size_ - read_size << "."
                  << std::endl;
        auto new_read_size = perform_file_read(filename.c_str(), finfo.offset_,
                                               ptr, total_read_size + read_size,
                                               hinfo.size_ - read_size);
        read_size += new_read_size;
      }
    } else if (fs::exists(filename) &&
               fs::file_size(filename) >= finfo.offset_ + finfo.size_) {
      LOG(INFO)
          << "Blob does not exists and need to read from original filename: "
          << filename << " offset:" << finfo.offset_
          << " size:" << finfo.size_ << "." << std::endl;
      read_size = perform_file_read(filename.c_str(), finfo.offset_, ptr,
                                    total_read_size, finfo.size_);
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
  mdm->Update(fp, stat);
  ret = total_read_size;

  return ret;
}

FILE *HERMES_DECL(fopen)(const char *path, const char *mode) {
  FILE *ret;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting fopen(" << path << ", " << mode << ")\n";
    ret = open_internal(path, mode);
  } else {
    MAP_OR_FAIL(fopen);
    ret = real_fopen_(path, mode);
  }

  return ret;
}

FILE *HERMES_DECL(fopen64)(const char *path, const char *mode) {
  FILE *ret;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting fopen64(" << path << ", " << mode << ")\n";
    ret = open_internal(path, mode);
  } else {
    MAP_OR_FAIL(fopen64);
    ret = real_fopen64_(path, mode);
  }

  return ret;
}

FILE *HERMES_DECL(fdopen)(int fd, const char *mode) {
  FILE *ret;
  MAP_OR_FAIL(fdopen);
  ret = real_fdopen_(fd, mode);
  if (ret && hermes::adapter::IsTracked(ret)) {
    std::string path_str = hermes::adapter::GetFilenameFromFD(fd);
    LOG(INFO) << "Intercepting fdopen(" << fd << ", " << mode << ")\n";
    const int kMaxSize = 0xFFF;
    char proclnk[kMaxSize];
    char filename[kMaxSize];
    snprintf(proclnk, kMaxSize, "/proc/self/fd/%d", fd);
    size_t r = readlink(proclnk, filename, kMaxSize);
    filename[r] = '\0';
    ret = simple_open(ret, filename, mode);
  }

  return ret;
}

FILE *HERMES_DECL(freopen)(const char *path, const char *mode, FILE *stream) {
  FILE *ret;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting freopen(" << path << ", " << mode << ", "
              << stream << ")\n";
    ret = reopen_internal(path, mode, stream);
  } else {
    MAP_OR_FAIL(freopen);
    ret = real_freopen_(path, mode, stream);
  }

  return ret;
}

FILE *HERMES_DECL(freopen64)(const char *path, const char *mode, FILE *stream) {
  FILE *ret;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting freopen64(" << path << ", " << mode << ", "
              << stream << ")\n";
    ret = reopen_internal(path, mode, stream);
  } else {
    MAP_OR_FAIL(freopen64);
    ret = real_freopen64_(path, mode, stream);
  }

  return ret;
}

int HERMES_DECL(fflush)(FILE *fp) {
  int ret = -1;
  if (fp && hermes::adapter::IsTracked(fp)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercepting fflush(" << fp << ")\n";
      auto filename = existing.first.st_bkid->GetName();
      const auto &blob_names = existing.first.st_blobs;
      if (!blob_names.empty() && INTERCEPTOR_LIST->Persists(fp)) {
        if (PersistEagerly(filename)) {
          existing.first.st_vbkt->WaitForBackgroundFlush();
        } else {
          LOG(INFO) << "File handler is opened by adapter." << std::endl;
          hapi::Context ctx;
          LOG(INFO) << "Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          hapi::VBucket file_vbucket(filename, mdm->GetHermes());
          auto offset_map = std::unordered_map<std::string, hermes::u64>();

          for (const auto &blob_name : blob_names) {
            file_vbucket.Link(blob_name, filename, ctx);
            auto page_index = std::stol(blob_name) - 1;
            offset_map.emplace(blob_name, page_index * kPageSize);
          }
          bool flush_synchronously = true;
          hapi::PersistTrait persist_trait(filename, offset_map,
                                           flush_synchronously);
          file_vbucket.Attach(&persist_trait);
          existing.first.st_blobs.clear();
          file_vbucket.Destroy();
        }
      }
      ret = 0;
    }
  } else {
    MAP_OR_FAIL(fflush);
    ret = real_fflush_(fp);
  }

  return ret;
}

int HERMES_DECL(fclose)(FILE *fp) {
  int ret;
  if (hermes::adapter::IsTracked(fp)) {
    LOG(INFO) << "Intercepting fclose(" << fp << ")\n";
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      if (existing.first.ref_count == 1) {
        auto persist = INTERCEPTOR_LIST->Persists(fp);
        auto filename = existing.first.st_bkid->GetName();
        mdm->Delete(fp);
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty() && persist) {
          if (PersistEagerly(filename)) {
            existing.first.st_vbkt->WaitForBackgroundFlush();
            existing.first.st_vbkt->Destroy();
          } else {
            LOG(INFO) << "Adapter flushes " << blob_names.size()
                      << " blobs to filename:" << filename << "." << std::endl;
            INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
            hapi::VBucket file_vbucket(filename, mdm->GetHermes());
            auto offset_map = std::unordered_map<std::string, hermes::u64>();

            for (const auto &blob_name : blob_names) {
              auto status = file_vbucket.Link(blob_name, filename);
              if (!status.Failed()) {
                auto page_index = std::stol(blob_name) - 1;
                offset_map.emplace(blob_name, page_index * kPageSize);
              }
            }
            bool flush_synchronously = true;
            hapi::PersistTrait persist_trait(filename, offset_map,
                                             flush_synchronously);
            file_vbucket.Attach(&persist_trait);
            existing.first.st_blobs.clear();
            INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
            file_vbucket.Destroy();
          }
        }
        existing.first.st_bkid->Destroy();
        mdm->FinalizeHermes();
      } else {
        LOG(INFO) << "File handler is opened by more than one fopen.\n";
        existing.first.ref_count--;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        existing.first.st_atim = ts;
        existing.first.st_ctim = ts;
        mdm->Update(fp, existing.first);
        existing.first.st_bkid->Release();
        if (existing.first.st_vbkt) {
          existing.first.st_vbkt->Release();
        }
      }
    }
  }

  MAP_OR_FAIL(fclose);
  ret = real_fclose_(fp);

  return ret;
}

size_t HERMES_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb,
                           FILE *fp) {
  size_t ret;
  if (hermes::adapter::IsTracked(fp)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercepting fwrite(" << ptr << ", " << size << ", "
                << nmemb << ", " << fp << ")\n";
      ret = write_internal(existing.first, ptr, size * nmemb, fp);
    } else {
      MAP_OR_FAIL(fwrite);
      ret = real_fwrite_(ptr, size, nmemb, fp);
    }
  } else {
    MAP_OR_FAIL(fwrite);
    ret = real_fwrite_(ptr, size, nmemb, fp);
  }

  return ret;
}

int HERMES_DECL(fputc)(int c, FILE *fp) {
  int ret;
  if (hermes::adapter::IsTracked(fp)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercepting fputc(" << c << ", " << fp << ")\n";
      write_internal(existing.first, &c, 1, fp);
      ret = c;
    } else {
      MAP_OR_FAIL(fputc);
      ret = real_fputc_(c, fp);
    }
  } else {
    MAP_OR_FAIL(fputc);
    ret = real_fputc_(c, fp);
  }

  return ret;
}

int HERMES_DECL(fgetpos)(FILE *fp, fpos_t *pos) {
  int ret;
  if (hermes::adapter::IsTracked(fp) && pos) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercept fgetpos." << std::endl;
      // TODO(chogan): @portability In the GNU C Library, fpos_t is an opaque
      // data structure that contains internal data to represent file offset and
      // conversion state information. In other systems, it might have a
      // different internal representation. This will need to change to support
      // other compilers.
      pos->__pos = existing.first.st_ptr;
      ret = 0;
    } else {
      MAP_OR_FAIL(fgetpos);
      ret = real_fgetpos_(fp, pos);
    }
  } else {
    MAP_OR_FAIL(fgetpos);
    ret = real_fgetpos_(fp, pos);
  }

  return ret;
}

int HERMES_DECL(fgetpos64)(FILE *fp, fpos64_t *pos) {
  int ret;
  if (hermes::adapter::IsTracked(fp) && pos) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercept fgetpos64." << std::endl;
      // TODO(chogan): @portability In the GNU C Library, fpos_t is an opaque
      // data structure that contains internal data to represent file offset and
      // conversion state information. In other systems, it might have a
      // different internal representation. This will need to change to support
      // other compilers.
      pos->__pos = existing.first.st_ptr;
      ret = 0;
    } else {
      MAP_OR_FAIL(fgetpos64);
      ret = real_fgetpos64_(fp, pos);
    }
  } else {
    MAP_OR_FAIL(fgetpos64);
    ret = real_fgetpos64_(fp, pos);
  }

  return ret;
}

int HERMES_DECL(putc)(int c, FILE *fp) {
  int ret;
  if (hermes::adapter::IsTracked(fp)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercept putc." << std::endl;
      write_internal(existing.first, &c, 1, fp);
      ret = c;
    } else {
      MAP_OR_FAIL(fputc);
      ret = real_fputc_(c, fp);
    }
  } else {
    MAP_OR_FAIL(fputc);
    ret = real_fputc_(c, fp);
  }

  return ret;
}

int HERMES_DECL(putw)(int w, FILE *fp) {
  int ret;
  if (hermes::adapter::IsTracked(fp)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercept putw." << std::endl;
      ret = write_internal(existing.first, &w, sizeof(w), fp);
      if (ret == sizeof(w)) {
        ret = 0;
      } else {
        ret = EOF;
      }
    } else {
      MAP_OR_FAIL(putw);
      ret = real_putw_(w, fp);
    }
  } else {
    MAP_OR_FAIL(putw);
    ret = real_putw_(w, fp);
  }

  return ret;
}

int HERMES_DECL(fputs)(const char *s, FILE *stream) {
  int ret;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fputs." << std::endl;
      ret = write_internal(existing.first, s, strlen(s), stream);
    } else {
      MAP_OR_FAIL(fputs);
      ret = real_fputs_(s, stream);
    }
  } else {
    MAP_OR_FAIL(fputs);
    ret = real_fputs_(s, stream);
  }

  return ret;
}

size_t HERMES_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  size_t ret;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fread with size: " << size << "." << std::endl;
      ret = read_internal(existing.first, ptr, size * nmemb, stream);
    } else {
      MAP_OR_FAIL(fread);
      ret = real_fread_(ptr, size, nmemb, stream);
    }
  } else {
    MAP_OR_FAIL(fread);
    ret = real_fread_(ptr, size, nmemb, stream);
  }

  return ret;
}

int HERMES_DECL(fgetc)(FILE *stream) {
  int ret = -1;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fgetc." << std::endl;
      unsigned char value;
      auto ret_size =
          read_internal(existing.first, &value, sizeof(unsigned char), stream);
      if (ret_size == sizeof(unsigned char)) {
        ret = value;
      }
    } else {
      MAP_OR_FAIL(fgetc);
      ret = real_fgetc_(stream);
    }
  } else {
    MAP_OR_FAIL(fgetc);
    ret = real_fgetc_(stream);
  }

  return ret;
}

int HERMES_DECL(getc)(FILE *stream) {
  int ret = -1;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept getc." << std::endl;
      unsigned char value;
      auto ret_size =
          read_internal(existing.first, &value, sizeof(unsigned char), stream);
      if (ret_size == sizeof(unsigned char)) {
        ret = value;
      }
    } else {
      MAP_OR_FAIL(fgetc);
      ret = real_fgetc_(stream);
    }
  } else {
    MAP_OR_FAIL(fgetc);
    ret = real_fgetc_(stream);
  }

  return ret;
}

int HERMES_DECL(getw)(FILE *stream) {
  int ret = -1;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept getw." << std::endl;
      int value;
      auto ret_size = read_internal(existing.first, &value, sizeof(int),
                                    stream);
      if (ret_size == sizeof(int)) {
        ret = value;
      }
    } else {
      MAP_OR_FAIL(getw);
      ret = real_getw_(stream);
    }
  } else {
    MAP_OR_FAIL(getw);
    ret = real_getw_(stream);
  }

  return ret;
}

char *HERMES_DECL(fgets)(char *s, int size, FILE *stream) {
  char *ret = nullptr;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fgets." << std::endl;
      size_t read_size = size - 1;
      size_t ret_size = read_internal(existing.first, s, read_size, stream);
      if (ret_size < read_size) {
        /* FILE ended */
        read_size = ret_size;
      }
      /* Check if \0 or \n in s.*/
      size_t copy_pos = 0;
      for (size_t i = 0; i < read_size; ++i) {
        if (s[i] == '\0' || s[i] == '\n') {
          copy_pos = i;
          break;
        }
      }
      if (copy_pos > 0) {
        /* \n and \0 was found. */
        s[copy_pos + 1] = '\0';
      } else {
        s[read_size] = '\0';
      }
      ret = s;
    } else {
      MAP_OR_FAIL(fgets);
      ret = real_fgets_(s, size, stream);
    }
  } else {
    MAP_OR_FAIL(fgets);
    ret = real_fgets_(s, size, stream);
  }

  return ret;
}

void HERMES_DECL(rewind)(FILE *stream) {
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept rewind." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        existing.first.st_ptr = 0;
        mdm->Update(stream, existing.first);
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
      }
    } else {
      MAP_OR_FAIL(rewind);
      real_rewind_(stream);
    }
  } else {
    MAP_OR_FAIL(rewind);
    real_rewind_(stream);
  }
}

int HERMES_DECL(fseek)(FILE *stream, long offset, int whence) {
  int ret = -1;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fseek offset:" << offset << " whence:" << whence
                << "." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        switch (whence) {
          case SEEK_SET: {
            existing.first.st_ptr = offset;
            break;
          }
          case SEEK_CUR: {
            existing.first.st_ptr += offset;
            break;
          }
          case SEEK_END: {
            existing.first.st_ptr = existing.first.st_size + offset;
            break;
          }
          default: {
            // TODO(hari): throw not implemented error.
          }
        }
        mdm->Update(stream, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(fseek);
      ret = real_fseek_(stream, offset, whence);
    }
  } else {
    MAP_OR_FAIL(fseek);
    ret = real_fseek_(stream, offset, whence);
  }

  return ret;
}

int HERMES_DECL(fseeko)(FILE *stream, off_t offset, int whence) {
  int ret;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fseeko offset:" << offset << " whence:" << whence
                << "." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        switch (whence) {
          case SEEK_SET: {
            existing.first.st_ptr = offset;
            break;
          }
          case SEEK_CUR: {
            existing.first.st_ptr += offset;
            break;
          }
          case SEEK_END: {
            existing.first.st_ptr = existing.first.st_size + offset;
            break;
          }
          default: {
            // TODO(hari): @errorhandling throw not implemented error.
          }
        }
        mdm->Update(stream, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(fseeko);
      ret = real_fseeko_(stream, offset, whence);
    }
  } else {
    MAP_OR_FAIL(fseeko);
    ret = real_fseeko_(stream, offset, whence);
  }

  return ret;
}

int HERMES_DECL(fseeko64)(FILE *stream, off64_t offset, int whence) {
  int ret;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fseeko64 offset:" << offset
                << " whence:" << whence << "." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        switch (whence) {
          case SEEK_SET: {
            existing.first.st_ptr = offset;
            break;
          }
          case SEEK_CUR: {
            existing.first.st_ptr += offset;
            break;
          }
          case SEEK_END: {
            existing.first.st_ptr = existing.first.st_size + offset;
            break;
          }
          default: {
            // TODO(hari): throw not implemented error.
          }
        }
        mdm->Update(stream, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(fseeko64);
      ret = real_fseeko64_(stream, offset, whence);
    }
  } else {
    MAP_OR_FAIL(fseeko64);
    ret = real_fseeko64_(stream, offset, whence);
  }

  return ret;
}

int HERMES_DECL(fsetpos)(FILE *stream, const fpos_t *pos) {
  int ret;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fsetpos offset:" << pos->__pos << "."
                << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        // TODO(chogan): @portability In the GNU C Library, fpos_t is an opaque
        // data structure that contains internal data to represent file offset
        // and conversion state information. In other systems, it might have a
        // different internal representation. This will need to change to
        // support other compilers.
        existing.first.st_ptr = pos->__pos;
        mdm->Update(stream, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(fsetpos);
      ret = real_fsetpos_(stream, pos);
    }
  } else {
    MAP_OR_FAIL(fsetpos);
    ret = real_fsetpos_(stream, pos);
  }

  return ret;
}

int HERMES_DECL(fsetpos64)(FILE *stream, const fpos64_t *pos) {
  int ret;
  if (hermes::adapter::IsTracked(stream)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "Intercept fsetpos64 offset:" << pos->__pos << "."
                << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
        // TODO(chogan): @portability In the GNU C Library, fpos_t is an opaque
        // data structure that contains internal data to represent file offset
        // and conversion state information. In other systems, it might have a
        // different internal representation. This will need to change to
        // support other compilers.
        existing.first.st_ptr = pos->__pos;
        mdm->Update(stream, existing.first);
        ret = 0;
      } else {
        LOG(INFO)
            << "File pointer not updating as file was opened in append mode."
            << std::endl;
        ret = -1;
      }
    } else {
      MAP_OR_FAIL(fsetpos64);
      ret = real_fsetpos64_(stream, pos);
    }
  } else {
    MAP_OR_FAIL(fsetpos64);
    ret = real_fsetpos64_(stream, pos);
  }

  return ret;
}

long int HERMES_DECL(ftell)(FILE *fp) {
  long int ret;
  if (hermes::adapter::IsTracked(fp)) {
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "Intercept ftell." << std::endl;
      ret = existing.first.st_ptr;
    } else {
      MAP_OR_FAIL(ftell);
      ret = real_ftell_(fp);
    }
  } else {
    MAP_OR_FAIL(ftell);
    ret = real_ftell_(fp);
  }

  return ret;
}
