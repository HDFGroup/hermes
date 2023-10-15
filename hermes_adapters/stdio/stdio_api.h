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
#include "hermes_shm/util/logging.h"
#include <cstdio>
#include "hermes_adapters/real_api.h"

extern "C" {
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

namespace hermes::adapter::fs {

/** Pointers to the real stdio API */
class StdioApi : public RealApi {
 public:
  /** fopen */
  fopen_t fopen = nullptr;
  /** fopen64 */
  fopen64_t fopen64 = nullptr;
  /** fdopen */
  fdopen_t fdopen = nullptr;
  /** freopen */
  freopen_t freopen = nullptr;
  /** freopen64 */
  freopen64_t freopen64 = nullptr;
  /** fflush */
  fflush_t fflush = nullptr;
  /** fclose */
  fclose_t fclose = nullptr;
  /** fwrite */
  fwrite_t fwrite = nullptr;
  /** fputc */
  fputc_t fputc = nullptr;
  /** fgetpos */
  fgetpos_t fgetpos = nullptr;
  /** fgetpos64 */
  fgetpos64_t fgetpos64 = nullptr;
  /** putc */
  putc_t putc = nullptr;
  /** putw */
  putw_t putw = nullptr;
  /** fputs */
  fputs_t fputs = nullptr;
  /** fread */
  fread_t fread = nullptr;
  /** fgetc */
  fgetc_t fgetc = nullptr;
  /** getc */
  getc_t getc = nullptr;
  /** getw */
  getw_t getw = nullptr;
  /** fgets */
  fgets_t fgets = nullptr;
  /** rewind */
  rewind_t rewind = nullptr;
  /** fseek */
  fseek_t fseek = nullptr;
  /** fseeko */
  fseeko_t fseeko = nullptr;
  /** fseeko64 */
  fseeko64_t fseeko64 = nullptr;
  /** fsetpos */
  fsetpos_t fsetpos = nullptr;
  /** fsetpos64 */
  fsetpos64_t fsetpos64 = nullptr;
  /** ftell */
  ftell_t ftell = nullptr;

  StdioApi() : RealApi("fopen", "stdio_intercepted") {
    fopen = (fopen_t)dlsym(real_lib_, "fopen");
    REQUIRE_API(fopen)
    fopen64 = (fopen64_t)dlsym(real_lib_, "fopen64");
    REQUIRE_API(fopen64)
    fdopen = (fdopen_t)dlsym(real_lib_, "fdopen");
    REQUIRE_API(fdopen)
    freopen = (freopen_t)dlsym(real_lib_, "freopen");
    REQUIRE_API(freopen)
    freopen64 = (freopen64_t)dlsym(real_lib_, "freopen64");
    REQUIRE_API(freopen64)
    fflush = (fflush_t)dlsym(real_lib_, "fflush");
    REQUIRE_API(fflush)
    fclose = (fclose_t)dlsym(real_lib_, "fclose");
    REQUIRE_API(fclose)
    fwrite = (fwrite_t)dlsym(real_lib_, "fwrite");
    REQUIRE_API(fwrite)
    fputc = (fputc_t)dlsym(real_lib_, "fputc");
    REQUIRE_API(fputc)
    fgetpos = (fgetpos_t)dlsym(real_lib_, "fgetpos");
    REQUIRE_API(fgetpos)
    fgetpos64 = (fgetpos64_t)dlsym(real_lib_, "fgetpos64");
    REQUIRE_API(fgetpos64)
    putc = (putc_t)dlsym(real_lib_, "putc");
    REQUIRE_API(putc)
    putw = (putw_t)dlsym(real_lib_, "putw");
    REQUIRE_API(putw)
    fputs = (fputs_t)dlsym(real_lib_, "fputs");
    REQUIRE_API(fputs)
    fread = (fread_t)dlsym(real_lib_, "fread");
    REQUIRE_API(fread)
    fgetc = (fgetc_t)dlsym(real_lib_, "fgetc");
    REQUIRE_API(fgetc)
    getc = (getc_t)dlsym(real_lib_, "getc");
    REQUIRE_API(getc)
    getw = (getw_t)dlsym(real_lib_, "getw");
    REQUIRE_API(getw)
    fgets = (fgets_t)dlsym(real_lib_, "fgets");
    REQUIRE_API(fgets)
    rewind = (rewind_t)dlsym(real_lib_, "rewind");
    REQUIRE_API(rewind)
    fseek = (fseek_t)dlsym(real_lib_, "fseek");
    REQUIRE_API(fseek)
    fseeko = (fseeko_t)dlsym(real_lib_, "fseeko");
    REQUIRE_API(fseeko)
    fseeko64 = (fseeko64_t)dlsym(real_lib_, "fseeko64");
    REQUIRE_API(fseeko64)
    fsetpos = (fsetpos_t)dlsym(real_lib_, "fsetpos");
    REQUIRE_API(fsetpos)
    fsetpos64 = (fsetpos64_t)dlsym(real_lib_, "fsetpos64");
    REQUIRE_API(fsetpos64)
    ftell = (ftell_t)dlsym(real_lib_, "ftell");
    REQUIRE_API(ftell)
  }
};
}  // namespace hermes::adapter::fs

#include "hermes_shm/util/singleton.h"

// Singleton macros
#define HERMES_STDIO_API \
  hshm::EasySingleton<::hermes::adapter::fs::StdioApi>::GetInstance()
#define HERMES_STDIO_API_T hermes::adapter::fs::StdioApi*

#endif  // HERMES_ADAPTER_STDIO_H
