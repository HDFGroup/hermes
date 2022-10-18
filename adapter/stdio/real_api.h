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
#include <cstdio>
#include "interceptor.h"
#include "filesystem/filesystem.h"
#include "filesystem/metadata_manager.h"

#define REQUIRE_API(api_name) \
  if (real_api->api_name == nullptr) { \
    LOG(FATAL) << "HERMES Adapter failed to map symbol: " \
    #api_name << std::endl; \
    exit(1);

extern "C" {
typedef int (*MPI_Init_t)(int * argc, char *** argv);
typedef int (*MPI_Finalize_t)( void);
typedef FILE * (*fopen_t)(const char * path, const char * mode);
typedef FILE * (*fopen64_t)(const char * path, const char * mode);
typedef FILE * (*fdopen_t)(int fd, const char * mode);
typedef FILE * (*freopen_t)(const char * path, const char * mode, FILE * stream);
typedef FILE * (*freopen64_t)(const char * path, const char * mode, FILE * stream);
typedef int (*fflush_t)(FILE * fp);
typedef int (*fclose_t)(FILE * fp);
typedef size_t (*fwrite_t)(const void * ptr, size_t size, size_t nmemb, FILE * fp);
typedef int (*fputc_t)(int c, FILE * fp);
typedef int (*fgetpos_t)(FILE * fp, fpos_t * pos);
typedef int (*fgetpos64_t)(FILE * fp, fpos64_t * pos);
typedef int (*putc_t)(int c, FILE * fp);
typedef int (*putw_t)(int w, FILE * fp);
typedef int (*fputs_t)(const char * s, FILE * stream);
typedef size_t (*fread_t)(void * ptr, size_t size, size_t nmemb, FILE * stream);
typedef int (*fgetc_t)(FILE * stream);
typedef int (*getc_t)(FILE * stream);
typedef int (*getw_t)(FILE * stream);
typedef char * (*fgets_t)(char * s, int size, FILE * stream);
typedef void (*rewind_t)(FILE * stream);
typedef int (*fseek_t)(FILE * stream, long offset, int whence);
typedef int (*fseeko_t)(FILE * stream, off_t offset, int whence);
typedef int (*fseeko64_t)(FILE * stream, off64_t offset, int whence);
typedef int (*fsetpos_t)(FILE * stream, const fpos_t * pos);
typedef int (*fsetpos64_t)(FILE * stream, const fpos64_t * pos);
typedef long int (*ftell_t)(FILE * fp);
}

namespace hermes::adapter::stdio {

class API {
 public:
  int (*MPI_Init)(int * argc, char *** argv) = nullptr;
  int (*MPI_Finalize)( void) = nullptr;
  FILE * (*fopen)(const char * path, const char * mode) = nullptr;
  FILE * (*fopen64)(const char * path, const char * mode) = nullptr;
  FILE * (*fdopen)(int fd, const char * mode) = nullptr;
  FILE * (*freopen)(const char * path, const char * mode, FILE * stream) = nullptr;
  FILE * (*freopen64)(const char * path, const char * mode, FILE * stream) = nullptr;
  int (*fflush)(FILE * fp) = nullptr;
  int (*fclose)(FILE * fp) = nullptr;
  size_t (*fwrite)(const void * ptr, size_t size, size_t nmemb, FILE * fp) = nullptr;
  int (*fputc)(int c, FILE * fp) = nullptr;
  int (*fgetpos)(FILE * fp, fpos_t * pos) = nullptr;
  int (*fgetpos64)(FILE * fp, fpos64_t * pos) = nullptr;
  int (*putc)(int c, FILE * fp) = nullptr;
  int (*putw)(int w, FILE * fp) = nullptr;
  int (*fputs)(const char * s, FILE * stream) = nullptr;
  size_t (*fread)(void * ptr, size_t size, size_t nmemb, FILE * stream) = nullptr;
  int (*fgetc)(FILE * stream) = nullptr;
  int (*getc)(FILE * stream) = nullptr;
  int (*getw)(FILE * stream) = nullptr;
  char * (*fgets)(char * s, int size, FILE * stream) = nullptr;
  void (*rewind)(FILE * stream) = nullptr;
  int (*fseek)(FILE * stream, long offset, int whence) = nullptr;
  int (*fseeko)(FILE * stream, off_t offset, int whence) = nullptr;
  int (*fseeko64)(FILE * stream, off64_t offset, int whence) = nullptr;
  int (*fsetpos)(FILE * stream, const fpos_t * pos) = nullptr;
  int (*fsetpos64)(FILE * stream, const fpos64_t * pos) = nullptr;
  long int (*ftell)(FILE * fp) = nullptr;

  API() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "stdio_intercepted");
    if (is_intercepted) {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_NEXT, "MPI_Init");
    } else {
      MPI_Init = (MPI_Init_t)dlsym(RTLD_DEFAULT, "MPI_Init");
    }
    if (is_intercepted) {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_NEXT, "MPI_Finalize");
    } else {
      MPI_Finalize = (MPI_Finalize_t)dlsym(RTLD_DEFAULT, "MPI_Finalize");
    }
    if (is_intercepted) {
      fopen = (fopen_t)dlsym(RTLD_NEXT, "fopen");
    } else {
      fopen = (fopen_t)dlsym(RTLD_DEFAULT, "fopen");
    }
    if (is_intercepted) {
      fopen64 = (fopen64_t)dlsym(RTLD_NEXT, "fopen64");
    } else {
      fopen64 = (fopen64_t)dlsym(RTLD_DEFAULT, "fopen64");
    }
    if (is_intercepted) {
      fdopen = (fdopen_t)dlsym(RTLD_NEXT, "fdopen");
    } else {
      fdopen = (fdopen_t)dlsym(RTLD_DEFAULT, "fdopen");
    }
    if (is_intercepted) {
      freopen = (freopen_t)dlsym(RTLD_NEXT, "freopen");
    } else {
      freopen = (freopen_t)dlsym(RTLD_DEFAULT, "freopen");
    }
    if (is_intercepted) {
      freopen64 = (freopen64_t)dlsym(RTLD_NEXT, "freopen64");
    } else {
      freopen64 = (freopen64_t)dlsym(RTLD_DEFAULT, "freopen64");
    }
    if (is_intercepted) {
      fflush = (fflush_t)dlsym(RTLD_NEXT, "fflush");
    } else {
      fflush = (fflush_t)dlsym(RTLD_DEFAULT, "fflush");
    }
    if (is_intercepted) {
      fclose = (fclose_t)dlsym(RTLD_NEXT, "fclose");
    } else {
      fclose = (fclose_t)dlsym(RTLD_DEFAULT, "fclose");
    }
    if (is_intercepted) {
      fwrite = (fwrite_t)dlsym(RTLD_NEXT, "fwrite");
    } else {
      fwrite = (fwrite_t)dlsym(RTLD_DEFAULT, "fwrite");
    }
    if (is_intercepted) {
      fputc = (fputc_t)dlsym(RTLD_NEXT, "fputc");
    } else {
      fputc = (fputc_t)dlsym(RTLD_DEFAULT, "fputc");
    }
    if (is_intercepted) {
      fgetpos = (fgetpos_t)dlsym(RTLD_NEXT, "fgetpos");
    } else {
      fgetpos = (fgetpos_t)dlsym(RTLD_DEFAULT, "fgetpos");
    }
    if (is_intercepted) {
      fgetpos64 = (fgetpos64_t)dlsym(RTLD_NEXT, "fgetpos64");
    } else {
      fgetpos64 = (fgetpos64_t)dlsym(RTLD_DEFAULT, "fgetpos64");
    }
    if (is_intercepted) {
      putc = (putc_t)dlsym(RTLD_NEXT, "putc");
    } else {
      putc = (putc_t)dlsym(RTLD_DEFAULT, "putc");
    }
    if (is_intercepted) {
      putw = (putw_t)dlsym(RTLD_NEXT, "putw");
    } else {
      putw = (putw_t)dlsym(RTLD_DEFAULT, "putw");
    }
    if (is_intercepted) {
      fputs = (fputs_t)dlsym(RTLD_NEXT, "fputs");
    } else {
      fputs = (fputs_t)dlsym(RTLD_DEFAULT, "fputs");
    }
    if (is_intercepted) {
      fread = (fread_t)dlsym(RTLD_NEXT, "fread");
    } else {
      fread = (fread_t)dlsym(RTLD_DEFAULT, "fread");
    }
    if (is_intercepted) {
      fgetc = (fgetc_t)dlsym(RTLD_NEXT, "fgetc");
    } else {
      fgetc = (fgetc_t)dlsym(RTLD_DEFAULT, "fgetc");
    }
    if (is_intercepted) {
      getc = (getc_t)dlsym(RTLD_NEXT, "getc");
    } else {
      getc = (getc_t)dlsym(RTLD_DEFAULT, "getc");
    }
    if (is_intercepted) {
      getw = (getw_t)dlsym(RTLD_NEXT, "getw");
    } else {
      getw = (getw_t)dlsym(RTLD_DEFAULT, "getw");
    }
    if (is_intercepted) {
      fgets = (fgets_t)dlsym(RTLD_NEXT, "fgets");
    } else {
      fgets = (fgets_t)dlsym(RTLD_DEFAULT, "fgets");
    }
    if (is_intercepted) {
      rewind = (rewind_t)dlsym(RTLD_NEXT, "rewind");
    } else {
      rewind = (rewind_t)dlsym(RTLD_DEFAULT, "rewind");
    }
    if (is_intercepted) {
      fseek = (fseek_t)dlsym(RTLD_NEXT, "fseek");
    } else {
      fseek = (fseek_t)dlsym(RTLD_DEFAULT, "fseek");
    }
    if (is_intercepted) {
      fseeko = (fseeko_t)dlsym(RTLD_NEXT, "fseeko");
    } else {
      fseeko = (fseeko_t)dlsym(RTLD_DEFAULT, "fseeko");
    }
    if (is_intercepted) {
      fseeko64 = (fseeko64_t)dlsym(RTLD_NEXT, "fseeko64");
    } else {
      fseeko64 = (fseeko64_t)dlsym(RTLD_DEFAULT, "fseeko64");
    }
    if (is_intercepted) {
      fsetpos = (fsetpos_t)dlsym(RTLD_NEXT, "fsetpos");
    } else {
      fsetpos = (fsetpos_t)dlsym(RTLD_DEFAULT, "fsetpos");
    }
    if (is_intercepted) {
      fsetpos64 = (fsetpos64_t)dlsym(RTLD_NEXT, "fsetpos64");
    } else {
      fsetpos64 = (fsetpos64_t)dlsym(RTLD_DEFAULT, "fsetpos64");
    }
    if (is_intercepted) {
      ftell = (ftell_t)dlsym(RTLD_NEXT, "ftell");
    } else {
      ftell = (ftell_t)dlsym(RTLD_DEFAULT, "ftell");
    }
  }
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_ADAPTER_STDIO_H
