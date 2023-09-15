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

bool stdio_intercepted = true;

#include <limits.h>
#include <sys/file.h>
#include <cstdio>
#include "stdio_api.h"
#include "stdio_fs_api.h"
#include "hermes_adapters/interceptor.h"

using hermes::adapter::fs::MetadataManager;
using hermes::adapter::fs::SeekMode;
using hermes::adapter::fs::AdapterStat;
using hermes::adapter::fs::File;

namespace stdfs = std::filesystem;

extern "C" {

/**
 * STDIO
 */

FILE *HERMES_DECL(fopen)(const char *path, const char *mode) {
  TRANSPARENT_HERMES();
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercepting fopen({}, {})", path, mode)
    AdapterStat stat;
    stat.mode_str_ = mode;
    return fs_api->Open(stat, path).hermes_fh_;
  } else {
    return real_api->fopen(path, mode);
  }
}

FILE *HERMES_DECL(fopen64)(const char *path, const char *mode) {
  TRANSPARENT_HERMES();
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsPathTracked(path)) {
    HILOG(kDebug, "Intercepting fopen64({}, {})", path, mode)
    AdapterStat stat;
    stat.mode_str_ = mode;
    return fs_api->Open(stat, path).hermes_fh_;
  } else {
    return real_api->fopen64(path, mode);
  }
}

FILE *HERMES_DECL(fdopen)(int fd, const char *mode) {
  TRANSPARENT_HERMES();
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  std::shared_ptr<AdapterStat> stat;
  if (fs_api->IsFdTracked(fd, stat)) {
    HILOG(kDebug, "Intercepting fdopen({})", fd, mode)
    return fs_api->FdOpen(mode, stat);
  } else {
    return real_api->fdopen(fd, mode);
  }
}

FILE *HERMES_DECL(freopen)(const char *path, const char *mode, FILE *stream) {
  TRANSPARENT_HERMES();
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting freopen({}, {})", path, mode)
    return fs_api->Reopen(path, mode, *(AdapterStat*)stream);
  }
  return real_api->freopen(path, mode, stream);
}

FILE *HERMES_DECL(freopen64)(const char *path, const char *mode, FILE *stream) {
  TRANSPARENT_HERMES();
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting freopen64({}, {})", path, mode)
    return fs_api->Reopen(path, mode, *(AdapterStat*)stream);
  }
  return real_api->freopen64(path, mode, stream);
}

int HERMES_DECL(fflush)(FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    HILOG(kDebug, "Intercepting fflush")
    File f;
    f.hermes_fh_ = fp;
    return fs_api->Sync(f, stat_exists);
  }
  return real_api->fflush(fp);
}

int HERMES_DECL(fclose)(FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    HILOG(kDebug, "Intercepting fclose({})", (void*)fp)
    File f; f.hermes_fh_ = fp;
    return fs_api->Close(f, stat_exists);
  }
  return real_api->fclose(fp);
}

size_t HERMES_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb,
                           FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    HILOG(kDebug, "Intercepting fwrite with size: {} and nmemb: {}",
          size, nmemb)
    File f; f.hermes_fh_ = fp;
    IoStatus io_status;
    int ret = fs_api->Write(f, stat_exists, ptr, size * nmemb, io_status);
    if (ret > 0) {
      return ret / size;
    } else {
      return ret;
    }
  }
  return real_api->fwrite(ptr, size, nmemb, fp);
}

int HERMES_DECL(fputc)(int c, FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    HILOG(kDebug, "Intercepting fputc({})", c)
    File f; f.hermes_fh_ = fp;
    IoStatus io_status;
    fs_api->Write(f, stat_exists, &c, 1, io_status);
    if (stat_exists) {
      return c;
    }
  }
  return real_api->fputc(c, fp);
}

int HERMES_DECL(fgetpos)(FILE *fp, fpos_t *pos) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp) && pos) {
    File f; f.hermes_fh_ = fp;
    HILOG(kDebug, "Intercepting fgetpos")
    // TODO(chogan): @portability In the GNU C Library, fpos_t is an opaque
    // data structure that contains internal data to represent file offset and
    // conversion state information. In other systems, it might have a
    // different internal representation. This will need to change to support
    // other compilers.
    pos->__pos = fs_api->Tell(f, stat_exists);
    if (stat_exists) {
      return 0;
    }
  }
  return real_api->fgetpos(fp, pos);
}

int HERMES_DECL(fgetpos64)(FILE *fp, fpos64_t *pos) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp) && pos) {
    File f; f.hermes_fh_ = fp;
    HILOG(kDebug, "Intercepting fgetpos64")
    // TODO(chogan): @portability In the GNU C Library, fpos_t is an opaque
    // data structure that contains internal data to represent file offset and
    // conversion state information. In other systems, it might have a
    // different internal representation. This will need to change to support
    // other compilers.
    pos->__pos = fs_api->Tell(f, stat_exists);
    return 0;
  }
  return real_api->fgetpos64(fp, pos);
}

int HERMES_DECL(putc)(int c, FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    File f; f.hermes_fh_ = fp;
    IoStatus io_status;
    HILOG(kDebug, "Intercepting putc")
    fs_api->Write(f, stat_exists, &c, 1, io_status);
    return c;
  }
  return real_api->fputc(c, fp);
}

int HERMES_DECL(putw)(int w, FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    HILOG(kDebug, "Intercepting putw")
    File f; f.hermes_fh_ = fp;
    IoStatus io_status;
    int ret = fs_api->Write(f, stat_exists, &w, sizeof(w), io_status);
    if (ret == sizeof(w)) {
      return 0;
    } else {
      return EOF;
    }
  }
  return real_api->putw(w, fp);
}

int HERMES_DECL(fputs)(const char *s, FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fputs")
    File f; f.hermes_fh_ = stream;
    IoStatus io_status;
    return fs_api->Write(f, stat_exists, s, strlen(s), io_status);
  }
  return real_api->fputs(s, stream);
}

size_t HERMES_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fread with size: {} and nmemb: {}",
          size, nmemb)
    File f; f.hermes_fh_ = stream;
    IoStatus io_status;
    int ret = fs_api->Read(f, stat_exists, ptr, size * nmemb, io_status);
    if (ret > 0) {
      return ret / size;
    } else {
      return ret;
    }
  }
  return real_api->fread(ptr, size, nmemb, stream);
}

int HERMES_DECL(fgetc)(FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fgetc")
    File f; f.hermes_fh_ = stream;
    IoStatus io_status;
    u8 value;
    fs_api->Read(f, stat_exists, &value, sizeof(u8), io_status);
    return value;
  }
  return real_api->fgetc(stream);
}

int HERMES_DECL(getc)(FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting getc")
    File f; f.hermes_fh_ = stream;
    IoStatus io_status;
    u8 value;
    fs_api->Read(f, stat_exists, &value, sizeof(u8), io_status);
    return value;
  }
  return real_api->getc(stream);
}

int HERMES_DECL(getw)(FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting getw")
    File f; f.hermes_fh_ = stream;
    IoStatus io_status;
    int value;
    fs_api->Read(f, stat_exists, &value, sizeof(int), io_status);
    return value;
  }
  return real_api->getc(stream);
}

char *HERMES_DECL(fgets)(char *s, int size, FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fgets")
    File f; f.hermes_fh_ = stream;
    IoStatus io_status;
    size_t read_size = size - 1;
    size_t ret_size = fs_api->Read(f, stat_exists, s, read_size, io_status);
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
    return s;
  }
  return real_api->fgets(s, size, stream);
}

void HERMES_DECL(rewind)(FILE *stream) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting rewind")
    File f; f.hermes_fh_ = stream;
    fs_api->Seek(f, stat_exists, SeekMode::kSet, 0);
    return;
  }
  real_api->rewind(stream);
}

int HERMES_DECL(fseek)(FILE *stream, long offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fseek offset: {} whence: {}",
          offset, whence)
    File f; f.hermes_fh_ = stream;
    fs_api->Seek(f, stat_exists, static_cast<SeekMode>(whence), offset);
    return 0;
  }
  return real_api->fseek(stream, offset, whence);
}

int HERMES_DECL(fseeko)(FILE *stream, off_t offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fseeko offset: {} whence: {}",
          offset, whence)
    File f; f.hermes_fh_ = stream;
    fs_api->Seek(f, stat_exists, static_cast<SeekMode>(whence), offset);
    return 0;
  }
  return real_api->fseeko(stream, offset, whence);
}

int HERMES_DECL(fseeko64)(FILE *stream, off64_t offset, int whence) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fseeko64 offset: {} whence: {}",
          offset, whence)
    File f; f.hermes_fh_ = stream;
    fs_api->Seek(f, stat_exists, static_cast<SeekMode>(whence), offset);
    return 0;
  }
  return real_api->fseeko64(stream, offset, whence);
}

int HERMES_DECL(fsetpos)(FILE *stream, const fpos_t *pos) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  off_t offset = pos->__pos;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fsetpos offset: {}",
          offset)
    File f; f.hermes_fh_ = stream;
    fs_api->Seek(f, stat_exists, SeekMode::kSet, offset);
    return 0;
  }
  return real_api->fsetpos(stream, pos);
}

int HERMES_DECL(fsetpos64)(FILE *stream, const fpos64_t *pos) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  off_t offset = pos->__pos;
  if (fs_api->IsFpTracked(stream)) {
    HILOG(kDebug, "Intercepting fsetpos64 offset: {}",
          offset)
    File f; f.hermes_fh_ = stream;
    fs_api->Seek(f, stat_exists, SeekMode::kSet, offset);
    return 0;
  }
  return real_api->fsetpos64(stream, pos);
}

long int HERMES_DECL(ftell)(FILE *fp) {
  bool stat_exists;
  auto real_api = HERMES_STDIO_API;
  auto fs_api = HERMES_STDIO_FS;
  if (fs_api->IsFpTracked(fp)) {
    HILOG(kDebug, "Intercepting ftell")
    File f; f.hermes_fh_ = fp;
    off_t ret = fs_api->Tell(f, stat_exists);
    return ret;
  }
  return real_api->ftell(fp);
}

}  // extern C
