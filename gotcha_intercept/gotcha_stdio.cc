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

extern "C" {
#include "gotcha_stdio.h"
}

#include <stdio.h>

#include "gotcha/gotcha.h"
#include "gotcha/gotcha_types.h"

#define FUNC_COUNT(wraps) ((sizeof(wraps) / sizeof(wraps[0])))

gotcha_wrappee_handle_t orig_fopen_handle;
typedef FILE * (hermes_fopen_fp)(const char *filename, const char * mode);
FILE * hermes_fopen(const char *filename, const char * mode);

gotcha_wrappee_handle_t orig_fclose_handle;
typedef int (hermes_fclose_fp)(FILE *stream);
int hermes_fclose(FILE *stream);

gotcha_wrappee_handle_t orig_fwrite_handle;
typedef size_t (hermes_fwrite_fp)(const void *ptr, size_t size, size_t count,
                                  FILE *stream);
size_t hermes_fwrite(const void *ptr, size_t size, size_t count, FILE *stream);

gotcha_wrappee_handle_t orig_fread_handle;
typedef size_t (hermes_fread_fp)(void * ptr, size_t size, size_t count,
                                 FILE *stream);
size_t hermes_fread(void * ptr, size_t size, size_t count, FILE *stream);

gotcha_wrappee_handle_t orig_fseek_handle;
typedef size_t (hermes_fseek_fp)(FILE * stream, long int offset, int origin);
size_t hermes_fseek(FILE * stream, long int offset, int origin);

struct gotcha_binding_t wrap_stdio[] = {
        { "fopen", (void *)hermes_fopen, &orig_fopen_handle },
        { "fclose", (void *)hermes_fclose, &orig_fclose_handle },
        { "fwrite", (void *)hermes_fwrite, &orig_fwrite_handle },
        { "fread", (void *)hermes_fread, &orig_fread_handle },
        { "fseek", (void *)hermes_fseek, &orig_fseek_handle },
};

void init_gotcha_stdio() {
  gotcha_wrap(wrap_stdio, FUNC_COUNT(wrap_stdio), "hermes");
}

FILE * hermes_fopen(const char *filename, const char * mode) {
  /** check if path matches prefix */
  bool path_match = 1;
  FILE *fp = nullptr;

  printf("In wrapper hermes_fopen\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function fopen()\n");
    fflush(stdout);
  }
  hermes_fopen_fp *orig_fopen =
    (hermes_fopen_fp *)gotcha_get_wrappee(orig_fopen_handle);
  fp = orig_fopen(filename, mode);

  return fp;
}

int hermes_fclose(FILE *stream) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_fclose\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function fclose()\n");
    fflush(stdout);
  }

  hermes_fclose_fp *orig_fclose =
    (hermes_fclose_fp *)gotcha_get_wrappee(orig_fclose_handle);
  ret = orig_fclose(stream);

  return ret;
}

size_t hermes_fwrite(const void *ptr, size_t size, size_t count, FILE *stream) {
  /** check if path matches prefix */
  bool path_match = 1;
  size_t ret = 0;

  printf("In wrapper hermes_fwrite\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function fwrite()\n");
    fflush(stdout);
  }

  hermes_fwrite_fp *orig_fwrite =
    (hermes_fwrite_fp *)gotcha_get_wrappee(orig_fwrite_handle);
  ret = orig_fwrite(ptr, size, count, stream);

  return ret;
}

size_t hermes_fread(void * ptr, size_t size, size_t count, FILE *stream) {
  /** check if path matches prefix */
  bool path_match = 1;
  size_t ret = 0;

  printf("In wrapper hermes_fread\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function fread()\n");
    fflush(stdout);
  }

  hermes_fread_fp *orig_fread =
    (hermes_fread_fp *)gotcha_get_wrappee(orig_fread_handle);
  ret = orig_fread(ptr, size, count, stream);

  return ret;
}

size_t hermes_fseek(FILE * stream, long int offset, int origin) {
  /** check if path matches prefix */
  bool path_match = 1;
  size_t ret = 0;

  printf("In wrapper hermes_fseek\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function fseek()\n");
    fflush(stdout);
  }

  hermes_fseek_fp *orig_fseek =
    (hermes_fseek_fp *)gotcha_get_wrappee(orig_fseek_handle);
  ret = orig_fseek(stream, offset, origin);

  return ret;
}
