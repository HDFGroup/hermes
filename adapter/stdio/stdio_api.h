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
#include "adapter/real_api.h"

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
class StdioApi {
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

  StdioApi() {
    void *is_intercepted = (void*)dlsym(RTLD_DEFAULT, "stdio_intercepted");
    if (is_intercepted) {
      fopen = (fopen_t)dlsym(RTLD_NEXT, "fopen");
    } else {
      fopen = (fopen_t)dlsym(RTLD_DEFAULT, "fopen");
    }
    REQUIRE_API(fopen)
    if (is_intercepted) {
      fopen64 = (fopen64_t)dlsym(RTLD_NEXT, "fopen64");
    } else {
      fopen64 = (fopen64_t)dlsym(RTLD_DEFAULT, "fopen64");
    }
    REQUIRE_API(fopen64)
    if (is_intercepted) {
      fdopen = (fdopen_t)dlsym(RTLD_NEXT, "fdopen");
    } else {
      fdopen = (fdopen_t)dlsym(RTLD_DEFAULT, "fdopen");
    }
    REQUIRE_API(fdopen)
    if (is_intercepted) {
      freopen = (freopen_t)dlsym(RTLD_NEXT, "freopen");
    } else {
      freopen = (freopen_t)dlsym(RTLD_DEFAULT, "freopen");
    }
    REQUIRE_API(freopen)
    if (is_intercepted) {
      freopen64 = (freopen64_t)dlsym(RTLD_NEXT, "freopen64");
    } else {
      freopen64 = (freopen64_t)dlsym(RTLD_DEFAULT, "freopen64");
    }
    REQUIRE_API(freopen64)
    if (is_intercepted) {
      fflush = (fflush_t)dlsym(RTLD_NEXT, "fflush");
    } else {
      fflush = (fflush_t)dlsym(RTLD_DEFAULT, "fflush");
    }
    REQUIRE_API(fflush)
    if (is_intercepted) {
      fclose = (fclose_t)dlsym(RTLD_NEXT, "fclose");
    } else {
      fclose = (fclose_t)dlsym(RTLD_DEFAULT, "fclose");
    }
    REQUIRE_API(fclose)
    if (is_intercepted) {
      fwrite = (fwrite_t)dlsym(RTLD_NEXT, "fwrite");
    } else {
      fwrite = (fwrite_t)dlsym(RTLD_DEFAULT, "fwrite");
    }
    REQUIRE_API(fwrite)
    if (is_intercepted) {
      fputc = (fputc_t)dlsym(RTLD_NEXT, "fputc");
    } else {
      fputc = (fputc_t)dlsym(RTLD_DEFAULT, "fputc");
    }
    REQUIRE_API(fputc)
    if (is_intercepted) {
      fgetpos = (fgetpos_t)dlsym(RTLD_NEXT, "fgetpos");
    } else {
      fgetpos = (fgetpos_t)dlsym(RTLD_DEFAULT, "fgetpos");
    }
    REQUIRE_API(fgetpos)
    if (is_intercepted) {
      fgetpos64 = (fgetpos64_t)dlsym(RTLD_NEXT, "fgetpos64");
    } else {
      fgetpos64 = (fgetpos64_t)dlsym(RTLD_DEFAULT, "fgetpos64");
    }
    REQUIRE_API(fgetpos64)
    if (is_intercepted) {
      putc = (putc_t)dlsym(RTLD_NEXT, "putc");
    } else {
      putc = (putc_t)dlsym(RTLD_DEFAULT, "putc");
    }
    REQUIRE_API(putc)
    if (is_intercepted) {
      putw = (putw_t)dlsym(RTLD_NEXT, "putw");
    } else {
      putw = (putw_t)dlsym(RTLD_DEFAULT, "putw");
    }
    REQUIRE_API(putw)
    if (is_intercepted) {
      fputs = (fputs_t)dlsym(RTLD_NEXT, "fputs");
    } else {
      fputs = (fputs_t)dlsym(RTLD_DEFAULT, "fputs");
    }
    REQUIRE_API(fputs)
    if (is_intercepted) {
      fread = (fread_t)dlsym(RTLD_NEXT, "fread");
    } else {
      fread = (fread_t)dlsym(RTLD_DEFAULT, "fread");
    }
    REQUIRE_API(fread)
    if (is_intercepted) {
      fgetc = (fgetc_t)dlsym(RTLD_NEXT, "fgetc");
    } else {
      fgetc = (fgetc_t)dlsym(RTLD_DEFAULT, "fgetc");
    }
    REQUIRE_API(fgetc)
    if (is_intercepted) {
      getc = (getc_t)dlsym(RTLD_NEXT, "getc");
    } else {
      getc = (getc_t)dlsym(RTLD_DEFAULT, "getc");
    }
    REQUIRE_API(getc)
    if (is_intercepted) {
      getw = (getw_t)dlsym(RTLD_NEXT, "getw");
    } else {
      getw = (getw_t)dlsym(RTLD_DEFAULT, "getw");
    }
    REQUIRE_API(getw)
    if (is_intercepted) {
      fgets = (fgets_t)dlsym(RTLD_NEXT, "fgets");
    } else {
      fgets = (fgets_t)dlsym(RTLD_DEFAULT, "fgets");
    }
    REQUIRE_API(fgets)
    if (is_intercepted) {
      rewind = (rewind_t)dlsym(RTLD_NEXT, "rewind");
    } else {
      rewind = (rewind_t)dlsym(RTLD_DEFAULT, "rewind");
    }
    REQUIRE_API(rewind)
    if (is_intercepted) {
      fseek = (fseek_t)dlsym(RTLD_NEXT, "fseek");
    } else {
      fseek = (fseek_t)dlsym(RTLD_DEFAULT, "fseek");
    }
    REQUIRE_API(fseek)
    if (is_intercepted) {
      fseeko = (fseeko_t)dlsym(RTLD_NEXT, "fseeko");
    } else {
      fseeko = (fseeko_t)dlsym(RTLD_DEFAULT, "fseeko");
    }
    REQUIRE_API(fseeko)
    if (is_intercepted) {
      fseeko64 = (fseeko64_t)dlsym(RTLD_NEXT, "fseeko64");
    } else {
      fseeko64 = (fseeko64_t)dlsym(RTLD_DEFAULT, "fseeko64");
    }
    REQUIRE_API(fseeko64)
    if (is_intercepted) {
      fsetpos = (fsetpos_t)dlsym(RTLD_NEXT, "fsetpos");
    } else {
      fsetpos = (fsetpos_t)dlsym(RTLD_DEFAULT, "fsetpos");
    }
    REQUIRE_API(fsetpos)
    if (is_intercepted) {
      fsetpos64 = (fsetpos64_t)dlsym(RTLD_NEXT, "fsetpos64");
    } else {
      fsetpos64 = (fsetpos64_t)dlsym(RTLD_DEFAULT, "fsetpos64");
    }
    REQUIRE_API(fsetpos64)
    if (is_intercepted) {
      ftell = (ftell_t)dlsym(RTLD_NEXT, "ftell");
    } else {
      ftell = (ftell_t)dlsym(RTLD_DEFAULT, "ftell");
    }
    REQUIRE_API(ftell)
  }
};
}  // namespace hermes::adapter::fs

#include "hermes_shm/util/singleton.h"

// Singleton macros
#define HERMES_STDIO_API \
  hshm::Singleton<hermes::adapter::fs::StdioApi>::GetInstance()
#define HERMES_STDIO_API_T hermes::adapter::fs::StdioApi*

#endif  // HERMES_ADAPTER_STDIO_H
