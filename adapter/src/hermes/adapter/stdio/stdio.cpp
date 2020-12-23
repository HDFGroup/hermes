//
// Created by manihariharan on 12/15/20.
//

#include <bucket.h>
#include <hermes.h>
#include <hermes/adapter/singleton.h>
#include <hermes/adapter/stdio.h>
#include <hermes/adapter/stdio/common/constants.h>
#include <hermes/adapter/stdio/common/datastructures.h>
#include <hermes/adapter/stdio/mapper/mapper_factory.h>
#include <hermes/adapter/stdio/metadata_manager.h>

using hermes::adapter::stdio::AdapterStat;
using hermes::adapter::stdio::FileID;
using hermes::adapter::stdio::FileStruct;
using hermes::adapter::stdio::MapperFactory;
using hermes::adapter::stdio::MetadataManager;

namespace hapi = hermes::api;

FILE *open_internal(const std::string &path_str, const char *mode) {
  FILE *ret;
  MAP_OR_FAIL(fopen);
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  ret = __real_fopen(path_str.c_str(), mode);
  if (!ret) {
    return ret;
  } else {
    auto existing = mdm->Find(ret);
    if (!existing.second) {
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
      /* FIXME(hari) check if this initialization is correct. */
      std::shared_ptr<hapi::Hermes> hermes =
          hapi::InitHermes(NULL, false, true);
      hapi::Context ctx;
      /* TODO(hari) how to pass to hermes to make a private bucket */
      stat.st_bkid = std::make_shared<hapi::Bucket>(path_str, hermes, ctx);
      mdm->Create(ret, stat);
    } else {
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

FILE *reopen_internal(const std::string &path_str, const char *mode,
                      FILE *stream) {
  FILE *ret;
  MAP_OR_FAIL(freopen);
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  ret = __real_freopen(path_str.c_str(), mode, stream);
  if (!ret) {
    return ret;
  } else {
    auto existing = mdm->Find(ret);
    if (!existing.second) {
      return nullptr;
    } else {
      struct timespec ts;
      timespec_get(&ts, TIME_UTC);
      existing.first.st_atim = ts;
      existing.first.st_ctim = ts;
      mdm->Update(ret, existing.first);
    }
  }
  return ret;
}

size_t write_internal(std::pair<AdapterStat, bool> existing, const void *ptr,
                      size_t total_size, FILE *fp) {
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto mapper = MapperFactory().Get(MAPPER_TYPE);
  auto mapping = mapper->map(FileStruct(existing.first.st_ptr, total_size));
  for (const auto &item : mapping) {
    hapi::Context ctx;
    hapi::Blob temp(0);
    auto exiting_blob_size =
        existing.first.st_bkid->Get(item.second.blob_name_, temp, ctx);
    hapi::Blob put_data((unsigned char *)ptr + item.first.offset_,
                        (unsigned char *)ptr + item.first.offset_ + total_size);
    if (exiting_blob_size == 0) {
      existing.first.st_bkid->Put(item.second.blob_name_, put_data, ctx);
    } else {
      if (item.second.offset_ == 0) {
        if (item.second.size_ >= exiting_blob_size) {
          existing.first.st_bkid->Put(item.second.blob_name_, put_data, ctx);
        } else {
          hapi::Blob existing_data(exiting_blob_size);
          existing.first.st_bkid->Get(item.second.blob_name_, existing_data,
                                      ctx);
          std::copy(put_data.begin(), put_data.end(), existing_data.begin());
          existing.first.st_bkid->Put(item.second.blob_name_, existing_data,
                                      ctx);
        }
      } else {
        auto new_size = item.second.offset_ + item.second.size_;
        hapi::Blob existing_data(exiting_blob_size);
        existing.first.st_bkid->Get(item.second.blob_name_, existing_data, ctx);
        if (new_size < exiting_blob_size) {
          new_size = exiting_blob_size;
        }
        hapi::Blob final_data(new_size);
        std::copy(existing_data.begin(),
                  existing_data.begin() + item.second.offset_ + 1,
                  final_data.begin());
        std::copy(put_data.begin(), put_data.end(),
                  final_data.begin() + item.second.offset_ + 1);
        if (new_size < exiting_blob_size) {
          std::copy(
              existing_data.begin() + item.second.offset_ + total_size + 1,
              existing_data.end(),
              final_data.begin() + total_size + item.second.offset_ + 1);
        }
      }
    }
  }
  existing.first.st_ptr += total_size;
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  existing.first.st_mtim = ts;
  existing.first.st_ctim = ts;
  mdm->Update(fp, existing.first);
  ret = total_size;
  return ret;
}

FILE *HERMES_DECL(fopen)(const char *path, const char *mode) {
  FILE *ret;
  std::string path_str(path);
  if (IsTracked(path)) {
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
  /* FIXME(hari): implement fdopen */
  ret = __real_fdopen(fd, mode);
  return (ret);
}

FILE *HERMES_DECL(freopen)(const char *path, const char *mode, FILE *stream) {
  FILE *ret;
  if (IsTracked(path)) {
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
    ret = reopen_internal(path, mode, stream);
  } else {
    MAP_OR_FAIL(freopen64);
    ret = __real_freopen64(path, mode, stream);
  }
  return (ret);
}

int HERMES_DECL(fflush)(FILE *fp) {
  int ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fp);
  if (existing.second) {
    /* existing.first.st_bkid-> TODO(hari): which is the flush api*/
  } else {
    MAP_OR_FAIL(fflush);
    ret = __real_fflush(fp);
  }
  return (ret);
}

int HERMES_DECL(fclose)(FILE *fp) {
  int ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fp);
  if (existing.second && existing.first.ref_count == 1) {
    hapi::Context ctx;
    existing.first.st_bkid->Close(ctx);
    mdm->Delete(fp);
  }
  MAP_OR_FAIL(fclose);
  ret = __real_fclose(fp);
  return (ret);
}

size_t HERMES_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb,
                           FILE *fp) {
  size_t ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fp);
  if (existing.second) {
    ret = write_internal(existing, ptr, size * nmemb, fp);
  } else {
    MAP_OR_FAIL(fwrite);
    ret = __real_fwrite(ptr, size, nmemb, fp);
  }
  return (ret);
}

int HERMES_DECL(fputc)(int c, FILE *fp) {
  int ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fp);
  if (existing.second) {
    ret = write_internal(existing, &c, 1, fp);
  } else {
    MAP_OR_FAIL(fputc);
    ret = __real_fputc(c, fp);
  }
  return (ret);
}

int HERMES_DECL(putw)(int w, FILE *fp) {
  int ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(fp);
  if (existing.second) {
    ret = write_internal(existing, &w, 1, fp);
  } else {
    MAP_OR_FAIL(putw);
    ret = __real_putw(w, fp);
  }
  return (ret);
}

int HERMES_DECL(fputs)(const char *s, FILE *stream) {
  int ret;
  auto mdm = hermes::adapter::Singleton<MetadataManager>::GetInstance();
  auto existing = mdm->Find(stream);
  if (existing.second) {
    ret = write_internal(existing, s, strlen(s), stream);
  } else {
    MAP_OR_FAIL(fputs);
    ret = __real_fputs(s, stream);
  }
  return (ret);
}

size_t HERMES_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  size_t ret;
  MAP_OR_FAIL(fread);
  ret = __real_fread(ptr, size, nmemb, stream);
  return (ret);
}

int HERMES_DECL(fgetc)(FILE *stream) {
  int ret;
  MAP_OR_FAIL(fgetc);
  ret = __real_fgetc(stream);
  return (ret);
}

/* NOTE: stdio.h typically implements getc() as a macro pointing to _IO_getc */
int HERMES_DECL(_IO_getc)(FILE *stream) {
  int ret;
  MAP_OR_FAIL(_IO_getc);
  ret = __real__IO_getc(stream);
  return (ret);
}

/* NOTE: stdio.h typically implements putc() as a macro pointing to _IO_putc */
int HERMES_DECL(_IO_putc)(int c, FILE *stream) {
  int ret;
  MAP_OR_FAIL(_IO_putc);
  ret = __real__IO_putc(c, stream);
  return (ret);
}

int HERMES_DECL(getw)(FILE *stream) {
  int ret;
  MAP_OR_FAIL(getw);
  ret = __real_getw(stream);
  return (ret);
}

char *HERMES_DECL(fgets)(char *s, int size, FILE *stream) {
  char *ret;
  MAP_OR_FAIL(fgets);
  ret = __real_fgets(s, size, stream);
  return (ret);
}

void HERMES_DECL(rewind)(FILE *stream) {
  MAP_OR_FAIL(rewind);
  __real_rewind(stream);
  return;
}

int HERMES_DECL(fseek)(FILE *stream, long offset, int whence) {
  int ret;
  MAP_OR_FAIL(fseek);
  ret = __real_fseek(stream, offset, whence);
  return (ret);
}

int HERMES_DECL(fseeko)(FILE *stream, off_t offset, int whence) {
  int ret;
  MAP_OR_FAIL(fseeko);
  ret = __real_fseeko(stream, offset, whence);
  return (ret);
}

int HERMES_DECL(fseeko64)(FILE *stream, off64_t offset, int whence) {
  int ret;
  MAP_OR_FAIL(fseeko64);
  ret = __real_fseeko64(stream, offset, whence);
  return (ret);
}

int HERMES_DECL(fsetpos)(FILE *stream, const fpos_t *pos) {
  int ret;
  MAP_OR_FAIL(fsetpos);
  ret = __real_fsetpos(stream, pos);
  return (ret);
}

int HERMES_DECL(fsetpos64)(FILE *stream, const fpos64_t *pos) {
  int ret;
  MAP_OR_FAIL(fsetpos64);
  ret = __real_fsetpos64(stream, pos);
  return (ret);
}

long int HERMES_DECL(ftell)(FILE *fp) {
  long int ret;
  MAP_OR_FAIL(ftell);

  ret = __real_ftell(fp);
  return (ret);
}
