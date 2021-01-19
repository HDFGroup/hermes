#include <fcntl.h>
#include <hermes/adapter/stdio.h>
#include <limits.h>

#include <hermes/adapter/interceptor.cc>
#include <hermes/adapter/stdio/mapper/balanced_mapper.cc>
#include <hermes/adapter/stdio/metadata_manager.cc>

using hermes::adapter::stdio::AdapterStat;
using hermes::adapter::stdio::FileID;
using hermes::adapter::stdio::FileStruct;
using hermes::adapter::stdio::MapperFactory;
using hermes::adapter::stdio::MetadataManager;

namespace hapi = hermes::api;
namespace fs = std::experimental::filesystem;

FILE *simple_open(FILE *ret, const std::string &path_str, const char *mode) {
  LOG(INFO) << "Open file for filename " << path_str << " in mode " << mode
            << std::endl;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  if (!ret) {
    return ret;
  } else {
    auto existing = mdm->Find(ret);
    if (!existing.second) {
      LOG(INFO) << "File not opened before by adapter" << std::endl;
      struct stat st;
      int fd = fileno(ret);
      fstat(fd, &st);
      AdapterStat stat(st);
      stat.ref_count = 1;
      struct timespec ts;
      timespec_get(&ts, TIME_UTC);
      stat.st_atim = ts;
      stat.st_mtim = ts;
      stat.st_ctim = ts;
      if (strcmp(mode, "a") == 0 || strcmp(mode, "a+") == 0) {
        /* FIXME: get current size of bucket from Hermes*/
        stat.st_ptr = stat.st_size;
      }
      /* FIXME(hari) check if this initialization is correct. */
      mdm->InitializeHermes();
      hapi::Context ctx;
      /* TODO(hari) how to pass to hermes to make a private bucket
       * also add how to handle existing buckets of same name */
      stat.st_bkid =
          std::make_shared<hapi::Bucket>(path_str, mdm->GetHermes(), ctx);
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
  }
  return ret;
}

FILE *open_internal(const std::string &path_str, const char *mode) {
  FILE *ret;
  MAP_OR_FAIL(fopen);
  ret = __real_fopen(path_str.c_str(), mode);
  ret = simple_open(ret, path_str.c_str(), mode);
  return ret;
}

FILE *reopen_internal(const std::string &path_str, const char *mode,
                      FILE *stream) {
  LOG(INFO) << "Reopen file for filename " << path_str << " in mode " << mode
            << std::endl;
  FILE *ret;
  MAP_OR_FAIL(freopen);
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  ret = __real_freopen(path_str.c_str(), mode, stream);
  if (!ret) {
    return ret;
  } else {
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

size_t write_internal(std::pair<AdapterStat, bool> &existing, const void *ptr,
                      size_t total_size, FILE *fp) {
  LOG(INFO) << "Write called for filename: "
            << existing.first.st_bkid->GetName()
            << " on offset: " << existing.first.st_ptr
            << " and size: " << total_size << std::endl;
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(MAPPER_TYPE);
  auto mapping = mapper->map(
      FileStruct(mdm->Convert(fp), existing.first.st_ptr, total_size));
  size_t data_offset = 0;
  auto filename = existing.first.st_bkid->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &item : mapping) {
    hapi::Context ctx;
    auto blob_exists =
        existing.first.st_bkid->ContainsBlob(item.second.blob_name_);
    hapi::Blob put_data((unsigned char *)ptr + data_offset,
                        (unsigned char *)ptr + data_offset + item.first.size_);
    existing.first.st_blobs.emplace(item.second.blob_name_);
    if (!blob_exists || item.second.size_ == PAGE_SIZE) {
      LOG(INFO) << "Overwrite blob " << item.second.blob_name_
                << " of size:" << item.second.size_ << "." << std::endl;
      existing.first.st_bkid->Put(item.second.blob_name_, put_data, ctx);
    } else {
      LOG(INFO) << "Blob " << item.second.blob_name_
                << " of size:" << item.second.size_ << " exists." << std::endl;
      hapi::Blob temp(0);
      auto exiting_blob_size =
          existing.first.st_bkid->Get(item.second.blob_name_, temp, ctx);
      if (item.second.offset_ == 0) {
        LOG(INFO) << "Blob offset is 0" << std::endl;
        if (item.second.size_ >= exiting_blob_size) {
          LOG(INFO) << "Overwrite blob " << item.second.blob_name_
                    << " of size:" << item.second.size_ << "." << std::endl;
          existing.first.st_bkid->Put(item.second.blob_name_, put_data, ctx);
        } else {
          LOG(INFO) << "Update blob " << item.second.blob_name_
                    << " of size:" << exiting_blob_size << "." << std::endl;
          hapi::Blob existing_data(exiting_blob_size);
          existing.first.st_bkid->Get(item.second.blob_name_, existing_data,
                                      ctx);
          memcpy(existing_data.data(), put_data.data(), put_data.size());
          existing.first.st_bkid->Put(item.second.blob_name_, existing_data,
                                      ctx);
        }
      } else {
        LOG(INFO) << "Blob offset: " << item.second.offset_ << "." << std::endl;
        auto new_size = item.second.offset_ + item.second.size_;
        hapi::Blob existing_data(exiting_blob_size);
        existing.first.st_bkid->Get(item.second.blob_name_, existing_data, ctx);
        if (new_size < exiting_blob_size) {
          new_size = exiting_blob_size;
        }
        hapi::Blob final_data(new_size);
        auto existing_data_cp_size =
            existing_data.size() >= item.second.offset_ + 1
                ? item.second.offset_ + 1
                : existing_data.size();
        memcpy(final_data.data(), existing_data.data(), existing_data_cp_size);

        if (exiting_blob_size < item.second.offset_ + 1 &&
            fs::exists(filename) &&
            fs::file_size(filename) >=
                item.second.offset_ + 1 + item.second.size_) {
          size_t size_to_read = item.second.offset_ + 1 - exiting_blob_size;
          LOG(INFO) << "Blob has a gap in update read gap from original file."
                    << std::endl;
          list.hermes_flush_exclusion.insert(filename);
          FILE *fh = fopen(filename.c_str(), "r");
          if (fh != nullptr) {
            if (fseek(fh, item.first.offset_ + 1, SEEK_SET) == 0) {
              size_t items_read =
                  fread(final_data.data() + existing_data_cp_size - 1,
                        size_to_read, sizeof(char), fh);
              if (items_read != 1) {
                // TODO(hari) @errorhandling read failed.
              }
              if (fclose(fh) != 0) {
                // TODO(hari) @errorhandling fclose failed.
              }
            } else {
              // TODO(hari) @errorhandling fseek failed.
            }
          } else {
            // TODO(hari) @errorhandling FILE cannot be opened
          }
          list.hermes_flush_exclusion.erase(filename);
        }
        memcpy(final_data.data() + item.second.offset_, put_data.data(),
               put_data.size());
        if (new_size < exiting_blob_size) {
          LOG(INFO) << "Retain last portion of blob as Blob is bigger than the "
                       "update."
                    << std::endl;
          auto off_t = total_size + item.second.offset_;
          memcpy(final_data.data() + off_t, existing_data.data() + off_t,
                 existing_data.size() - off_t + 1);
        }
        existing.first.st_bkid->Put(item.second.blob_name_, final_data, ctx);
      }
    }
    data_offset += item.first.size_;
  }
  existing.first.st_ptr += data_offset;
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  existing.first.st_mtim = ts;
  existing.first.st_ctim = ts;
  mdm->Update(fp, existing.first);
  ret = data_offset;
  return ret;
}

size_t perform_file_read(const char *filename, size_t file_offset, void *ptr,
                         size_t ptr_offset, size_t size) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << file_offset << " and size: " << size
            << std::endl;
  list.hermes_flush_exclusion.insert(filename);
  FILE *fh = fopen(filename, "r");
  size_t read_size = 0;
  if (fh != nullptr) {
    int status = fseek(fh, file_offset, SEEK_SET);
    if (status == 0) {
      read_size = fread((char *)ptr + ptr_offset, sizeof(char), size, fh);
    }
    status = fclose(fh);
  }
  list.hermes_flush_exclusion.erase(filename);
  return read_size;
}

size_t read_internal(std::pair<AdapterStat, bool> &existing, void *ptr,
                     size_t total_size, FILE *fp) {
  LOG(INFO) << "Write called for filename: "
            << existing.first.st_bkid->GetName()
            << " on offset: " << existing.first.st_ptr
            << " and size: " << total_size << std::endl;
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(MAPPER_TYPE);
  auto mapping = mapper->map(
      FileStruct(mdm->Convert(fp), existing.first.st_ptr, total_size));
  size_t total_read_size = 0;
  auto filename = existing.first.st_bkid->GetName();
  LOG(INFO) << "Mapping for read has " << mapping.size() << " mapping."
            << std::endl;
  for (const auto &item : mapping) {
    hapi::Context ctx;
    auto blob_exists =
        existing.first.st_bkid->ContainsBlob(item.second.blob_name_);
    hapi::Blob read_data(0);
    size_t read_size = 0;
    if (blob_exists) {
      LOG(INFO) << "Blob exists and need to read from Hermes from blob: "
                << item.second.blob_name_ << "." << std::endl;
      auto exiting_blob_size =
          existing.first.st_bkid->Get(item.second.blob_name_, read_data, ctx);

      read_data.resize(exiting_blob_size);
      existing.first.st_bkid->Get(item.second.blob_name_, read_data, ctx);
      bool contains_blob = exiting_blob_size > item.second.offset_;
      if (contains_blob) {
        read_size = read_data.size() < item.second.offset_ + item.second.size_
                        ? exiting_blob_size - item.second.offset_
                        : item.second.size_;
        LOG(INFO) << "Blob have data and need to read from hemes "
                     "blob: "
                  << item.second.blob_name_ << " offset:" << item.second.offset_
                  << " size:" << read_size << "." << std::endl;
        memcpy((char *)ptr + total_read_size,
               read_data.data() + item.second.offset_, read_size);
        if (read_size < item.second.size_) {
          contains_blob = true;
        } else {
          contains_blob = false;
        }
      } else {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << item.first.offset_
                  << " size:" << item.first.size_ << "." << std::endl;
        auto file_read_size =
            perform_file_read(filename.c_str(), item.first.offset_, ptr,
                              total_read_size, item.second.size_);
        read_size += file_read_size;
      }
      if (contains_blob && fs::exists(filename) &&
          fs::file_size(filename) >= item.first.offset_ + item.first.size_) {
        LOG(INFO) << "Blob does not have data and need to read from original "
                     "filename: "
                  << filename << " offset:" << item.first.offset_ + read_size
                  << " size:" << item.second.size_ - read_size << "."
                  << std::endl;
        auto new_read_size = perform_file_read(
            filename.c_str(), item.first.offset_, ptr,
            total_read_size + read_size, item.second.size_ - read_size);
        read_size += new_read_size;
      }
    } else if (fs::exists(filename) &&
               fs::file_size(filename) >=
                   item.first.offset_ + item.first.size_) {
      LOG(INFO)
          << "Blob does not exists and need to read from original filename: "
          << filename << " offset:" << item.first.offset_
          << " size:" << item.first.size_ << "." << std::endl;
      read_size = perform_file_read(filename.c_str(), item.first.offset_, ptr,
                                    total_read_size, item.first.size_);
    }
    if (read_size > 0) {
      total_read_size += read_size;
    }
  }
  existing.first.st_ptr += total_read_size;
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  existing.first.st_atim = ts;
  existing.first.st_ctim = ts;
  mdm->Update(fp, existing.first);
  ret = total_read_size;
  return ret;
}

FILE *HERMES_DECL(fopen)(const char *path, const char *mode) {
  FILE *ret;
  std::string path_str(path);
  if (IsTracked(path)) {
    LOG(INFO) << "Intercept fopen for filename: " << path
              << " and mode: " << mode << "." << std::endl;
    LOG(INFO) << "Filename: " << path << " is tracked." << std::endl;
    ret = open_internal(path, mode);
  } else {
    MAP_OR_FAIL(fopen);
    ret = __real_fopen(path, mode);
  }
  return (ret);
}

FILE *HERMES_DECL(fopen64)(const char *path, const char *mode) {
  FILE *ret;
  std::string path_str(path);
  if (IsTracked(path)) {
    LOG(INFO) << "Intercept fopen64 for filename: " << path
              << " and mode: " << mode << "." << std::endl;
    LOG(INFO) << "Filename: " << path << " is tracked." << std::endl;
    ret = open_internal(path, mode);
  } else {
    MAP_OR_FAIL(fopen64);
    ret = __real_fopen(path, mode);
  }
  return (ret);
}

FILE *HERMES_DECL(fdopen)(int fd, const char *mode) {
  FILE *ret;
  MAP_OR_FAIL(fdopen);
  ret = __real_fdopen(fd, mode);
  if (ret && IsTracked(ret)) {
    LOG(INFO) << "Intercept fdopen for file descriptor: " << fd
              << " and mode: " << mode << "." << std::endl;
    LOG(INFO) << "Intercept is tracked." << std::endl;
    int MAXSIZE = 0xFFF;
    char proclnk[0xFFF];
    char filename[0xFFF];
    snprintf(proclnk, MAXSIZE, "/proc/self/fd/%d", fd);
    size_t r = readlink(proclnk, filename, MAXSIZE);
    filename[r] = '\0';
    ret = simple_open(ret, filename, mode);
  }
  return (ret);
}

FILE *HERMES_DECL(freopen)(const char *path, const char *mode, FILE *stream) {
  FILE *ret;
  if (IsTracked(path)) {
    LOG(INFO) << "Intercept freopen for filename: " << path
              << " and mode: " << mode << "." << std::endl;
    LOG(INFO) << "Filename: " << path << " is tracked." << std::endl;
    ret = reopen_internal(path, mode, stream);
  } else {
    MAP_OR_FAIL(freopen);
    ret = __real_freopen(path, mode, stream);
  }
  return (ret);
}

FILE *HERMES_DECL(freopen64)(const char *path, const char *mode, FILE *stream) {
  FILE *ret;
  if (IsTracked(path)) {
    LOG(INFO) << "Intercept freopen64 for filename: " << path
              << " and mode: " << mode << "." << std::endl;
    LOG(INFO) << "Filename: " << path << " is tracked." << std::endl;
    ret = reopen_internal(path, mode, stream);
  } else {
    MAP_OR_FAIL(freopen64);
    ret = __real_freopen64(path, mode, stream);
  }
  return (ret);
}

int HERMES_DECL(fflush)(FILE *fp) {
  int ret;
  if (fp && IsTracked(fp)) {
    LOG(INFO) << "Intercept fflush." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      auto filename = existing.first.st_bkid->GetName();
      const auto &blob_names = existing.first.st_blobs;
      if (!blob_names.empty()) {
        list.hermes_flush_exclusion.insert(filename);
        LOG(INFO) << "File handler is opened by adapter." << std::endl;
        hapi::Context ctx;
        LOG(INFO) << "Adapter flushes " << blob_names.size()
                  << " blobs to filename:" << filename << "." << std::endl;
        hermes::api::VBucket file_vbucket(filename, mdm->GetHermes(), true,
                                          ctx);
        auto offset_map = std::unordered_map<std::string, hermes::u64>();
        for (const auto &blob_name : blob_names) {
          file_vbucket.Link(blob_name, filename, ctx);
          offset_map.emplace(blob_name, std::stol(blob_name) * PAGE_SIZE);
        }
        auto trait = hermes::api::FileMappingTrait(filename, offset_map,
                                                   nullptr, NULL, NULL);
        file_vbucket.Attach(&trait, ctx);
        file_vbucket.Delete(ctx);
        existing.first.st_blobs.clear();
        list.hermes_flush_exclusion.erase(filename);
      }
      ret = 0;
    }
  } else {
    MAP_OR_FAIL(fflush);
    ret = __real_fflush(fp);
  }
  return (ret);
}

int HERMES_DECL(fclose)(FILE *fp) {
  int ret;
  if (IsTracked(fp)) {
    LOG(INFO) << "Intercept fclose." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      if (existing.first.ref_count == 1) {
        mdm->Delete(fp);
        hapi::Context ctx;
        const auto &blob_names = existing.first.st_blobs;
        if (!blob_names.empty()) {
          auto filename = existing.first.st_bkid->GetName();
          LOG(INFO) << "Adapter flushes " << blob_names.size()
                    << " blobs to filename:" << filename << "." << std::endl;
          list.hermes_flush_exclusion.insert(filename);
          hermes::api::VBucket file_vbucket(filename, mdm->GetHermes(), true,
                                            ctx);
          auto offset_map = std::unordered_map<std::string, hermes::u64>();
          for (const auto &blob_name : blob_names) {
            file_vbucket.Link(blob_name, filename, ctx);
            offset_map.emplace(blob_name, std::stol(blob_name) * PAGE_SIZE);
          }
          auto trait = hermes::api::FileMappingTrait(filename, offset_map,
                                                     nullptr, NULL, NULL);
          file_vbucket.Attach(&trait, ctx);
          file_vbucket.Delete(ctx);
          existing.first.st_blobs.clear();
          list.hermes_flush_exclusion.erase(filename);
        }
        existing.first.st_bkid->Close(ctx);
        mdm->FinalizeHermes();
      } else {
        LOG(INFO) << "File handler is opened by more than one fopen."
                  << std::endl;
        existing.first.ref_count--;
        struct timespec ts;
        timespec_get(&ts, TIME_UTC);
        existing.first.st_atim = ts;
        existing.first.st_ctim = ts;
        mdm->Update(fp, existing.first);
      }
    }
  }

  MAP_OR_FAIL(fclose);
  ret = __real_fclose(fp);
  return (ret);
}

size_t HERMES_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb,
                           FILE *fp) {
  size_t ret;
  if (IsTracked(fp)) {
    LOG(INFO) << "Intercept fwrite." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      ret = write_internal(existing, ptr, size * nmemb, fp);
    } else {
      MAP_OR_FAIL(fwrite);
      ret = __real_fwrite(ptr, size, nmemb, fp);
    }
  } else {
    MAP_OR_FAIL(fwrite);
    ret = __real_fwrite(ptr, size, nmemb, fp);
  }
  return (ret);
}

int HERMES_DECL(fputc)(int c, FILE *fp) {
  int ret;
  if (IsTracked(fp)) {
    LOG(INFO) << "Intercept fputc." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      write_internal(existing, &c, 1, fp);
      ret = c;
    } else {
      MAP_OR_FAIL(fputc);
      ret = __real_fputc(c, fp);
    }
  } else {
    MAP_OR_FAIL(fputc);
    ret = __real_fputc(c, fp);
  }
  return (ret);
}

int HERMES_DECL(fgetpos)(FILE *fp, fpos_t *pos) {
  int ret;
  if (IsTracked(fp) && pos) {
    LOG(INFO) << "Intercept fgetpos." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      pos->__pos = existing.first.st_ptr;
      ret = 0;
    } else {
      MAP_OR_FAIL(fgetpos);
      ret = __real_fgetpos(fp, pos);
    }
  } else {
    MAP_OR_FAIL(fgetpos);
    ret = __real_fgetpos(fp, pos);
  }
  return ret;
}

int HERMES_DECL(putc)(int c, FILE *fp) {
  int ret;
  if (IsTracked(fp)) {
    LOG(INFO) << "Intercept putc." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      write_internal(existing, &c, 1, fp);
      ret = c;
    } else {
      MAP_OR_FAIL(fputc);
      ret = __real_fputc(c, fp);
    }
  } else {
    MAP_OR_FAIL(fputc);
    ret = __real_fputc(c, fp);
  }
  return (ret);
}

int HERMES_DECL(putw)(int w, FILE *fp) {
  int ret;
  if (IsTracked(fp)) {
    LOG(INFO) << "Intercept putw." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      ret = write_internal(existing, &w, 1, fp);
    } else {
      MAP_OR_FAIL(putw);
      ret = __real_putw(w, fp);
    }
  } else {
    MAP_OR_FAIL(putw);
    ret = __real_putw(w, fp);
  }
  return (ret);
}

int HERMES_DECL(fputs)(const char *s, FILE *stream) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fputs." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      ret = write_internal(existing, s, strlen(s), stream);
    } else {
      MAP_OR_FAIL(fputs);
      ret = __real_fputs(s, stream);
    }
  } else {
    MAP_OR_FAIL(fputs);
    ret = __real_fputs(s, stream);
  }
  return (ret);
}

size_t HERMES_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  size_t ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fread with size: " << size << "." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      ret = read_internal(existing, ptr, size * nmemb, stream);
    } else {
      MAP_OR_FAIL(fread);
      ret = __real_fread(ptr, size, nmemb, stream);
    }
  } else {
    MAP_OR_FAIL(fread);
    ret = __real_fread(ptr, size, nmemb, stream);
  }
  return (ret);
}

int HERMES_DECL(fgetc)(FILE *stream) {
  int ret = -1;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fgetc." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      unsigned char value;
      auto ret_size = read_internal(existing, &value, sizeof(value), stream);
      if (ret_size == 1) {
        ret = value;
      }
    } else {
      MAP_OR_FAIL(fgetc);
      ret = __real_fgetc(stream);
    }
  } else {
    MAP_OR_FAIL(fgetc);
    ret = __real_fgetc(stream);
  }
  return (ret);
}

int HERMES_DECL(getc)(FILE *stream) {
  int ret = -1;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept getc." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      unsigned char value;
      auto ret_size = read_internal(existing, &value, sizeof(value), stream);
      if (ret_size == 1) {
        ret = value;
      }
    } else {
      MAP_OR_FAIL(fgetc);
      ret = __real_fgetc(stream);
    }
  } else {
    MAP_OR_FAIL(fgetc);
    ret = __real_fgetc(stream);
  }
  return (ret);
}

/* NOTE: stdio.h typically implements getc() as a macro pointing to _IO_getc */
int HERMES_DECL(_IO_getc)(FILE *stream) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept _IO_getc." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      unsigned char value;
      auto ret_size = read_internal(existing, &value, sizeof(value), stream);
      if (ret_size == 1) {
        ret = value;
      }
    } else {
      MAP_OR_FAIL(_IO_getc);
      ret = __real__IO_getc(stream);
    }
  } else {
    MAP_OR_FAIL(_IO_getc);
    ret = __real__IO_getc(stream);
  }
  return (ret);
}

/* NOTE: stdio.h typically implements putc() as a macro pointing to _IO_putc */
int HERMES_DECL(_IO_putc)(int c, FILE *stream) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept _IO_putc char:" << (char *)&c << "." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      ret = write_internal(existing, &c, 1, stream);
    } else {
      MAP_OR_FAIL(_IO_putc);
      ret = __real__IO_putc(c, stream);
    }
  } else {
    MAP_OR_FAIL(_IO_putc);
    ret = __real__IO_putc(c, stream);
  }
  return (ret);
}

int HERMES_DECL(getw)(FILE *stream) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept getw." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      unsigned char value;
      auto ret_size = read_internal(existing, &value, sizeof(value), stream);
      if (ret_size == 1) ret = value;
    } else {
      MAP_OR_FAIL(getw);
      ret = __real_getw(stream);
    }
  } else {
    MAP_OR_FAIL(getw);
    ret = __real_getw(stream);
  }
  return (ret);
}

char *HERMES_DECL(fgets)(char *s, int size, FILE *stream) {
  char *ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fgets." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      read_internal(existing, s, size, stream);
      ret = s;
    } else {
      MAP_OR_FAIL(fgets);
      ret = __real_fgets(s, size, stream);
    }
  } else {
    MAP_OR_FAIL(fgets);
    ret = __real_fgets(s, size, stream);
  }
  return (ret);
}

void HERMES_DECL(rewind)(FILE *stream) {
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept rewind." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
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
      __real_rewind(stream);
    }
  } else {
    MAP_OR_FAIL(rewind);
    __real_rewind(stream);
  }
  return;
}

int HERMES_DECL(fseek)(FILE *stream, long offset, int whence) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fseek offset:" << offset << " whence:" << whence
              << "." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
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
      ret = __real_fseek(stream, offset, whence);
    }
  } else {
    MAP_OR_FAIL(fseek);
    ret = __real_fseek(stream, offset, whence);
  }
  return (ret);
}

int HERMES_DECL(fseeko)(FILE *stream, off_t offset, int whence) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fseeko offset:" << offset << " whence:" << whence
              << "." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
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
      MAP_OR_FAIL(fseeko);
      ret = __real_fseeko(stream, offset, whence);
    }
  } else {
    MAP_OR_FAIL(fseeko);
    ret = __real_fseeko(stream, offset, whence);
  }
  return (ret);
}

int HERMES_DECL(fseeko64)(FILE *stream, off64_t offset, int whence) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fseeko64 offset:" << offset << " whence:" << whence
              << "." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
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
      ret = __real_fseeko64(stream, offset, whence);
    }
  } else {
    MAP_OR_FAIL(fseeko64);
    ret = __real_fseeko64(stream, offset, whence);
  }
  return (ret);
}

int HERMES_DECL(fsetpos)(FILE *stream, const fpos_t *pos) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fsetpos offset:" << pos->__pos << "." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
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
      ret = __real_fsetpos(stream, pos);
    }
  } else {
    MAP_OR_FAIL(fsetpos);
    ret = __real_fsetpos(stream, pos);
  }
  return (ret);
}

int HERMES_DECL(fsetpos64)(FILE *stream, const fpos64_t *pos) {
  int ret;
  if (IsTracked(stream)) {
    LOG(INFO) << "Intercept fsetpos64 offset:" << pos->__pos << "."
              << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(stream);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      if (!(existing.first.st_mode & O_APPEND)) {
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
      ret = __real_fsetpos64(stream, pos);
    }
  } else {
    MAP_OR_FAIL(fsetpos64);
    ret = __real_fsetpos64(stream, pos);
  }
  return (ret);
}

long int HERMES_DECL(ftell)(FILE *fp) {
  long int ret;
  if (IsTracked(fp)) {
    LOG(INFO) << "Intercept ftell." << std::endl;
    LOG(INFO) << "File handler is tracked." << std::endl;
    auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(fp);
    if (existing.second) {
      LOG(INFO) << "File handler is opened by adapter." << std::endl;
      ret = existing.first.st_ptr;
    } else {
      MAP_OR_FAIL(ftell);
      ret = __real_ftell(fp);
    }
  } else {
    MAP_OR_FAIL(ftell);
    ret = __real_ftell(fp);
  }
  return (ret);
}
