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

#include "interceptor.cc"
#include "adapter_utils.cc"

#include <cstdio>
#include "stdio/real_api.h"
#include "stdio/fs_api.h"

using hermes::adapter::WeaklyCanonical;
using hermes::adapter::stdio::API;
using hermes::adapter::stdio::StdioFS;
using hermes::adapter::Singleton;
using hermes::adapter::fs::MetadataManager;

namespace hapi = hermes::api;
namespace stdfs = std::experimental::filesystem;
using hermes::u8;
using hermes::u64;

namespace hapi = hermes::api;
namespace stdfs = std::experimental::filesystem;


/**
 * MPI
 */
int HERMES_DECL(MPI_Init)(int *argc, char ***argv) {
  auto real_api = Singleton<API>::GetInstance();
  int status = real_api->MPI_Init(argc, argv);
  if (status == 0) {
    auto mdm = Singleton<MetadataManager>::GetInstance();
    mdm->InitializeHermes(true);
    LOG(INFO) << "MPI Init intercepted." << std::endl;
  }
  return status;
}

int HERMES_DECL(MPI_Finalize)(void) {
  LOG(INFO) << "MPI Finalize intercepted." << std::endl;
  auto real_api = Singleton<API>::GetInstance();
  auto mdm = Singleton<MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  int status = real_api->MPI_Finalize();
  return status;
}

/**
 * STDIO
 */

FILE *reopen_internal(const std::string &user_path, const char *mode,
                      FILE *stream) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  auto mdm = Singleton<MetadataManager>::GetInstance();
  FILE *ret;
  ret = real_api->freopen(user_path.c_str(), mode, stream);
  if (!ret) { return ret; }

  File f; f.fh_ = stream; fs_api->_InitFile(f);
  std::string path_str = WeaklyCanonical(user_path).string();
  LOG(INFO) << "Reopen file for filename " << path_str << " in mode " << mode
            << std::endl;
  auto existing = mdm->Find(f);
  if (!existing.second) {
    LOG(INFO) << "File not opened before by adapter" << std::endl;
    return nullptr;
  } else {
    LOG(INFO) << "File opened before by adapter" << std::endl;
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    existing.first.st_atim = ts;
    existing.first.st_ctim = ts;
    mdm->Update(f, existing.first);
  }
  return ret;
}

FILE *HERMES_DECL(fopen)(const char *path, const char *mode) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting fopen(" << path << ", " << mode << ")\n";
    AdapterStat stat;
    stat.mode_str = mode;
    return fs_api->Open(stat, path).fh_;
  } else {
    return real_api->fopen(path, mode);
  }
}

FILE *HERMES_DECL(fopen64)(const char *path, const char *mode) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting fopen64(" << path << ", " << mode << ")\n";
    AdapterStat stat; stat.mode_str = mode;
    return fs_api->Open(stat, path).fh_;
  } else {
    return real_api->fopen64(path, mode);
  }
}

FILE *HERMES_DECL(fdopen)(int fd, const char *mode) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  FILE *ret = real_api->fdopen(fd, mode);
  if (ret && hermes::adapter::IsTracked(ret)) {
    LOG(INFO) << "Intercepting fdopen(" << fd << ", " << mode << ")\n";
    std::string path_str = hermes::adapter::GetFilenameFromFD(fd);
    File f; f.fh_ = ret;
    AdapterStat stat; stat.mode_str = mode;
    fs_api->Open(stat, f, path_str);
  }
  return ret;
}

FILE *HERMES_DECL(freopen)(const char *path, const char *mode, FILE *stream) {
  auto real_api = Singleton<API>::GetInstance();
  //auto fs_api = Singleton<StdioFS>::GetInstance();
  FILE *ret;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting freopen(" << path << ", " << mode << ", "
              << stream << ")\n";
    ret = reopen_internal(path, mode, stream);
  } else {
    ret = real_api->freopen(path, mode, stream);
  }
  return ret;
}

FILE *HERMES_DECL(freopen64)(const char *path, const char *mode, FILE *stream) {
  auto real_api = Singleton<API>::GetInstance();
  //auto fs_api = Singleton<StdioFS>::GetInstance();
  FILE *ret;
  if (hermes::adapter::IsTracked(path)) {
    LOG(INFO) << "Intercepting freopen64(" << path << ", " << mode << ", "
              << stream << ")\n";
    ret = reopen_internal(path, mode, stream);
  } else {
    ret = real_api->freopen64(path, mode, stream);
  }

  return ret;
}

int HERMES_DECL(fflush)(FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  int ret = -1;
  if (fp && hermes::adapter::IsTracked(fp)) {
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    return fs_api->Sync(f, stat_exists);
  } else {
    ret = real_api->fflush(fp);
  }
  return ret;
}

int HERMES_DECL(fclose)(FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(fp)) {
    LOG(INFO) << "Intercepting fclose(" << fp << ")\n";
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    int ret = fs_api->Close(f, stat_exists);
    if (stat_exists) return ret;
  }
  return real_api->fclose(fp);
}

size_t HERMES_DECL(fwrite)(const void *ptr, size_t size, size_t nmemb,
                           FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(fp)) {
    LOG(INFO) << "Intercepting fwrite(" << ptr << ", " << size << ", "
              << nmemb << ", " << fp << ")\n";
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    size_t ret = fs_api->Write(f, stat_exists, ptr, size*nmemb);
    if (stat_exists) { return ret; }
  }
  return real_api->fwrite(ptr, size, nmemb, fp);
}

int HERMES_DECL(fputc)(int c, FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(fp)) {
      LOG(INFO) << "Intercepting fputc(" << c << ", " << fp << ")\n";
      File f; f.fh_ = fp; fs_api->_InitFile(f);
      fs_api->Write(f, stat_exists, &c, 1);
      if (stat_exists) { return c; }
  }
  return real_api->fputc(c, fp);
}

int HERMES_DECL(fgetpos)(FILE *fp, fpos_t *pos) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  int ret;
  if (hermes::adapter::IsTracked(fp) && pos) {
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    auto mdm = Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
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
      ret = real_api->fgetpos(fp, pos);
    }
  } else {
    ret = real_api->fgetpos(fp, pos);
  }

  return ret;
}

int HERMES_DECL(fgetpos64)(FILE *fp, fpos64_t *pos) {
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  int ret;
  if (hermes::adapter::IsTracked(fp) && pos) {
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    auto mdm = Singleton<MetadataManager>::GetInstance();
    auto existing = mdm->Find(f);
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
      ret = real_api->fgetpos64(fp, pos);
    }
  } else {
    ret = real_api->fgetpos64(fp, pos);
  }

  return ret;
}

int HERMES_DECL(putc)(int c, FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(fp)) {
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    LOG(INFO) << "Intercept putc." << std::endl;
    fs_api->Write(f, stat_exists, &c, 1);
    if (stat_exists) { return c; }
  }
  return real_api->fputc(c, fp);
}

int HERMES_DECL(putw)(int w, FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(fp)) {
    LOG(INFO) << "Intercept putw." << std::endl;
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    int ret = fs_api->Write(f, stat_exists, &w, sizeof(w));
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
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fputs." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    int ret = fs_api->Write(f, stat_exists, s, strlen(s));
    if (stat_exists) { return ret; }
  }
  return real_api->fputs(s, stream);;
}

size_t HERMES_DECL(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  size_t ret;
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fread with size: " << size << "." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    ret = fs_api->Read(f, stat_exists, ptr, size * nmemb);
    if (stat_exists) { return ret; }
  }
  return real_api->fread(ptr, size, nmemb, stream);
}

int HERMES_DECL(fgetc)(FILE *stream) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fgetc." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    u8 value;
    fs_api->Read(f, stat_exists, &value, sizeof(u8));
    if (stat_exists) { return value; }
  }
  return real_api->fgetc(stream);
}

int HERMES_DECL(getc)(FILE *stream) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept getc." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    u8 value;
    fs_api->Read(f, stat_exists, &value, sizeof(u8));
    if (stat_exists) { return value; }
  }
  return real_api->getc(stream);
}

int HERMES_DECL(getw)(FILE *stream) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept getw." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    int value;
    fs_api->Read(f, stat_exists, &value, sizeof(int));
    if (stat_exists) { return value; }
  }
  return real_api->getc(stream);
}

char *HERMES_DECL(fgets)(char *s, int size, FILE *stream) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fgets." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    size_t read_size = size - 1;
    size_t ret_size = fs_api->Read(f, stat_exists, s, read_size);
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
    if (stat_exists) return s;
  }
  return real_api->fgets(s, size, stream);
}

void HERMES_DECL(rewind)(FILE *stream) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept rewind." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    fs_api->Seek(f, stat_exists, SEEK_SET, 0);
    if (stat_exists) { return; }
  }
  real_api->rewind(stream);
}

int HERMES_DECL(fseek)(FILE *stream, long offset, int whence) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fseek offset:" << offset << " whence:" << whence
              << "." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    off_t ret = fs_api->Seek(f, stat_exists, whence, offset);
    if (stat_exists && ret > 0) { return 0; }
  }
  return real_api->fseek(stream, offset, whence);
}

int HERMES_DECL(fseeko)(FILE *stream, off_t offset, int whence) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fseeko offset:" << offset << " whence:" << whence
              << "." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    off_t ret = fs_api->Seek(f, stat_exists, whence, offset);
    if (stat_exists && ret > 0) { return 0; }
  }
  return real_api->fseeko(stream, offset, whence);
}

int HERMES_DECL(fseeko64)(FILE *stream, off64_t offset, int whence) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fseeko offset:" << offset << " whence:" << whence
              << "." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    off_t ret = fs_api->Seek(f, stat_exists, whence, offset);
    if (stat_exists && ret > 0) { return 0; }
  }
  return real_api->fseeko64(stream, offset, whence);
}

int HERMES_DECL(fsetpos)(FILE *stream, const fpos_t *pos) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  off_t offset = pos->__pos;
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fsetpos offset:" << offset << "." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    off_t ret = fs_api->Seek(f, stat_exists, SEEK_SET, offset);
    if (stat_exists && ret > 0) { return 0; }
  }
  return real_api->fsetpos(stream, pos);
}

int HERMES_DECL(fsetpos64)(FILE *stream, const fpos64_t *pos) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  off_t offset = pos->__pos;
  if (hermes::adapter::IsTracked(stream)) {
    LOG(INFO) << "Intercept fsetpos64 offset:" << offset << "." << std::endl;
    File f; f.fh_ = stream; fs_api->_InitFile(f);
    off_t ret = fs_api->Seek(f, stat_exists, SEEK_SET, offset);
    if (stat_exists && ret > 0) { return 0; }
  }
  return real_api->fsetpos64(stream, pos);
}

long int HERMES_DECL(ftell)(FILE *fp) {
  bool stat_exists;
  auto real_api = Singleton<API>::GetInstance();
  auto fs_api = Singleton<StdioFS>::GetInstance();
  if (hermes::adapter::IsTracked(fp)) {
    LOG(INFO) << "Intercept ftell." << std::endl;
    File f; f.fh_ = fp; fs_api->_InitFile(f);
    off_t ret = fs_api->Tell(f, stat_exists);
    if (stat_exists) { return ret; }
  }
  return real_api->ftell(fp);
}
