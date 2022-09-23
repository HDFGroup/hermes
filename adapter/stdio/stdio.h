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

#ifndef HERMES_ADAPTER_STDIO_H
#define HERMES_ADAPTER_STDIO_H
#include <string>
#include <dlfcn.h>
#include <iostream>
#include <glog/logging.h>
#include "interceptor.h"
#include "filesystem/filesystem.h"

namespace hermes::adapter::stdio {

class API {
 public:
  typedef int (*MPI_Init_t)(int * argc, char *** argv);
  int (*MPI_Init)(int * argc, char *** argv) = nullptr;
  typedef int (*MPI_Finalize_t)( void);
  int (*MPI_Finalize)( void) = nullptr;
  typedef FILE * (*fopen_t)(const char * path, const char * mode);
  FILE * (*fopen)(const char * path, const char * mode) = nullptr;
  typedef FILE * (*fopen64_t)(const char * path, const char * mode);
  FILE * (*fopen64)(const char * path, const char * mode) = nullptr;
  typedef FILE * (*fdopen_t)(int fd, const char * mode);
  FILE * (*fdopen)(int fd, const char * mode) = nullptr;
  typedef FILE * (*freopen_t)(const char * path, const char * mode, FILE * stream);
  FILE * (*freopen)(const char * path, const char * mode, FILE * stream) = nullptr;
  typedef FILE * (*freopen64_t)(const char * path, const char * mode, FILE * stream);
  FILE * (*freopen64)(const char * path, const char * mode, FILE * stream) = nullptr;
  typedef int (*fflush_t)(FILE * fp);
  int (*fflush)(FILE * fp) = nullptr;
  typedef int (*fclose_t)(FILE * fp);
  int (*fclose)(FILE * fp) = nullptr;
  typedef size_t (*fwrite_t)(const void * ptr, size_t size, size_t nmemb, FILE * fp);
  size_t (*fwrite)(const void * ptr, size_t size, size_t nmemb, FILE * fp) = nullptr;
  typedef int (*fputc_t)(int c, FILE * fp);
  int (*fputc)(int c, FILE * fp) = nullptr;
  typedef int (*fgetpos_t)(FILE * fp, fpos_t * pos);
  int (*fgetpos)(FILE * fp, fpos_t * pos) = nullptr;
  typedef int (*fgetpos64_t)(FILE * fp, fpos64_t * pos);
  int (*fgetpos64)(FILE * fp, fpos64_t * pos) = nullptr;
  typedef int (*putc_t)(int c, FILE * fp);
  int (*putc)(int c, FILE * fp) = nullptr;
  typedef int (*putw_t)(int w, FILE * fp);
  int (*putw)(int w, FILE * fp) = nullptr;
  typedef int (*fputs_t)(const char * s, FILE * stream);
  int (*fputs)(const char * s, FILE * stream) = nullptr;
  typedef size_t (*fread_t)(void * ptr, size_t size, size_t nmemb, FILE * stream);
  size_t (*fread)(void * ptr, size_t size, size_t nmemb, FILE * stream) = nullptr;
  typedef int (*fgetc_t)(FILE * stream);
  int (*fgetc)(FILE * stream) = nullptr;
  typedef int (*getc_t)(FILE * stream);
  int (*getc)(FILE * stream) = nullptr;
  typedef int (*getw_t)(FILE * stream);
  int (*getw)(FILE * stream) = nullptr;
  typedef char * (*fgets_t)(char * s, int size, FILE * stream);
  char * (*fgets)(char * s, int size, FILE * stream) = nullptr;
  typedef void (*rewind_t)(FILE * stream);
  void (*rewind)(FILE * stream) = nullptr;
  typedef int (*fseek_t)(FILE * stream, long offset, int whence);
  int (*fseek)(FILE * stream, long offset, int whence) = nullptr;
  typedef int (*fseeko_t)(FILE * stream, off_t offset, int whence);
  int (*fseeko)(FILE * stream, off_t offset, int whence) = nullptr;
  typedef int (*fseeko64_t)(FILE * stream, off64_t offset, int whence);
  int (*fseeko64)(FILE * stream, off64_t offset, int whence) = nullptr;
  typedef int (*fsetpos_t)(FILE * stream, const fpos_t * pos);
  int (*fsetpos)(FILE * stream, const fpos_t * pos) = nullptr;
  typedef int (*fsetpos64_t)(FILE * stream, const fpos64_t * pos);
  int (*fsetpos64)(FILE * stream, const fpos64_t * pos) = nullptr;
  typedef long int (*ftell_t)(FILE * fp);
  long int (*ftell)(FILE * fp) = nullptr;
  API() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "stdio_intercepted");
    if (is_intercepted) {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    if (MPI_Init == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Init" << std::endl;
    }
    if (is_intercepted) {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    if (MPI_Finalize == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "MPI_Finalize" << std::endl;
    }
    if (is_intercepted) {
      fopen = (fopen_t)dlsym(RTLD_NEXT, "fopen");
    } else {
      fopen = (fopen_t)dlsym(RTLD_DEFAULT, "fopen");
    }
    if (fopen == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fopen" << std::endl;
    }
    if (is_intercepted) {
      fopen64 = (fopen64_t)dlsym(RTLD_NEXT, "fopen64");
    } else {
      fopen64 = (fopen64_t)dlsym(RTLD_DEFAULT, "fopen64");
    }
    if (fopen64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fopen64" << std::endl;
    }
    if (is_intercepted) {
      fdopen = (fdopen_t)dlsym(RTLD_NEXT, "fdopen");
    } else {
      fdopen = (fdopen_t)dlsym(RTLD_DEFAULT, "fdopen");
    }
    if (fdopen == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fdopen" << std::endl;
    }
    if (is_intercepted) {
      freopen = (freopen_t)dlsym(RTLD_NEXT, "freopen");
    } else {
      freopen = (freopen_t)dlsym(RTLD_DEFAULT, "freopen");
    }
    if (freopen == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "freopen" << std::endl;
    }
    if (is_intercepted) {
      freopen64 = (freopen64_t)dlsym(RTLD_NEXT, "freopen64");
    } else {
      freopen64 = (freopen64_t)dlsym(RTLD_DEFAULT, "freopen64");
    }
    if (freopen64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "freopen64" << std::endl;
    }
    if (is_intercepted) {
      fflush = (fflush_t)dlsym(RTLD_NEXT, "fflush");
    } else {
      fflush = (fflush_t)dlsym(RTLD_DEFAULT, "fflush");
    }
    if (fflush == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fflush" << std::endl;
    }
    if (is_intercepted) {
      fclose = (fclose_t)dlsym(RTLD_NEXT, "fclose");
    } else {
      fclose = (fclose_t)dlsym(RTLD_DEFAULT, "fclose");
    }
    if (fclose == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fclose" << std::endl;
    }
    if (is_intercepted) {
      fwrite = (fwrite_t)dlsym(RTLD_NEXT, "fwrite");
    } else {
      fwrite = (fwrite_t)dlsym(RTLD_DEFAULT, "fwrite");
    }
    if (fwrite == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fwrite" << std::endl;
    }
    if (is_intercepted) {
      fputc = (fputc_t)dlsym(RTLD_NEXT, "fputc");
    } else {
      fputc = (fputc_t)dlsym(RTLD_DEFAULT, "fputc");
    }
    if (fputc == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fputc" << std::endl;
    }
    if (is_intercepted) {
      fgetpos = (fgetpos_t)dlsym(RTLD_NEXT, "fgetpos");
    } else {
      fgetpos = (fgetpos_t)dlsym(RTLD_DEFAULT, "fgetpos");
    }
    if (fgetpos == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fgetpos" << std::endl;
    }
    if (is_intercepted) {
      fgetpos64 = (fgetpos64_t)dlsym(RTLD_NEXT, "fgetpos64");
    } else {
      fgetpos64 = (fgetpos64_t)dlsym(RTLD_DEFAULT, "fgetpos64");
    }
    if (fgetpos64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fgetpos64" << std::endl;
    }
    if (is_intercepted) {
      putc = (putc_t)dlsym(RTLD_NEXT, "putc");
    } else {
      putc = (putc_t)dlsym(RTLD_DEFAULT, "putc");
    }
    if (putc == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "putc" << std::endl;
    }
    if (is_intercepted) {
      putw = (putw_t)dlsym(RTLD_NEXT, "putw");
    } else {
      putw = (putw_t)dlsym(RTLD_DEFAULT, "putw");
    }
    if (putw == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "putw" << std::endl;
    }
    if (is_intercepted) {
      fputs = (fputs_t)dlsym(RTLD_NEXT, "fputs");
    } else {
      fputs = (fputs_t)dlsym(RTLD_DEFAULT, "fputs");
    }
    if (fputs == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fputs" << std::endl;
    }
    if (is_intercepted) {
      fread = (fread_t)dlsym(RTLD_NEXT, "fread");
    } else {
      fread = (fread_t)dlsym(RTLD_DEFAULT, "fread");
    }
    if (fread == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fread" << std::endl;
    }
    if (is_intercepted) {
      fgetc = (fgetc_t)dlsym(RTLD_NEXT, "fgetc");
    } else {
      fgetc = (fgetc_t)dlsym(RTLD_DEFAULT, "fgetc");
    }
    if (fgetc == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fgetc" << std::endl;
    }
    if (is_intercepted) {
      getc = (getc_t)dlsym(RTLD_NEXT, "getc");
    } else {
      getc = (getc_t)dlsym(RTLD_DEFAULT, "getc");
    }
    if (getc == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "getc" << std::endl;
    }
    if (is_intercepted) {
      getw = (getw_t)dlsym(RTLD_NEXT, "getw");
    } else {
      getw = (getw_t)dlsym(RTLD_DEFAULT, "getw");
    }
    if (getw == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "getw" << std::endl;
    }
    if (is_intercepted) {
      fgets = (fgets_t)dlsym(RTLD_NEXT, "fgets");
    } else {
      fgets = (fgets_t)dlsym(RTLD_DEFAULT, "fgets");
    }
    if (fgets == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fgets" << std::endl;
    }
    if (is_intercepted) {
      rewind = (rewind_t)dlsym(RTLD_NEXT, "rewind");
    } else {
      rewind = (rewind_t)dlsym(RTLD_DEFAULT, "rewind");
    }
    if (rewind == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "rewind" << std::endl;
    }
    if (is_intercepted) {
      fseek = (fseek_t)dlsym(RTLD_NEXT, "fseek");
    } else {
      fseek = (fseek_t)dlsym(RTLD_DEFAULT, "fseek");
    }
    if (fseek == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fseek" << std::endl;
    }
    if (is_intercepted) {
      fseeko = (fseeko_t)dlsym(RTLD_NEXT, "fseeko");
    } else {
      fseeko = (fseeko_t)dlsym(RTLD_DEFAULT, "fseeko");
    }
    if (fseeko == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fseeko" << std::endl;
    }
    if (is_intercepted) {
      fseeko64 = (fseeko64_t)dlsym(RTLD_NEXT, "fseeko64");
    } else {
      fseeko64 = (fseeko64_t)dlsym(RTLD_DEFAULT, "fseeko64");
    }
    if (fseeko64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fseeko64" << std::endl;
    }
    if (is_intercepted) {
      fsetpos = (fsetpos_t)dlsym(RTLD_NEXT, "fsetpos");
    } else {
      fsetpos = (fsetpos_t)dlsym(RTLD_DEFAULT, "fsetpos");
    }
    if (fsetpos == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fsetpos" << std::endl;
    }
    if (is_intercepted) {
      fsetpos64 = (fsetpos64_t)dlsym(RTLD_NEXT, "fsetpos64");
    } else {
      fsetpos64 = (fsetpos64_t)dlsym(RTLD_DEFAULT, "fsetpos64");
    }
    if (fsetpos64 == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "fsetpos64" << std::endl;
    }
    if (is_intercepted) {
      ftell = (ftell_t)dlsym(RTLD_NEXT, "ftell");
    } else {
      ftell = (ftell_t)dlsym(RTLD_DEFAULT, "ftell");
    }
    if (ftell == nullptr) {
      LOG(FATAL) << "HERMES Adapter failed to map symbol: "
      "ftell" << std::endl;
    }
  }
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_ADAPTER_STDIO_H
