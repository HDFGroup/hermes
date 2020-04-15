#ifndef GOTCHA_STDIO_H_
#define GOTCHA_STDIO_H_

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
typedef size_t (hermes_fwrite_fp)(const void *ptr, size_t size, size_t count, FILE *stream);
size_t hermes_fwrite( const void *ptr, size_t size, size_t count, FILE *stream);

gotcha_wrappee_handle_t orig_fread_handle;
typedef size_t (hermes_fread_fp)(void * ptr, size_t size, size_t count, FILE *stream);
size_t hermes_fread(void * ptr, size_t size, size_t count, FILE *stream);

gotcha_wrappee_handle_t orig_fseek_handle;
typedef size_t (hermes_fseek_fp)(FILE * stream, long int offset, int origin);
size_t hermes_fseek(FILE * stream, long int offset, int origin);

struct gotcha_binding_t wrap_stdio [] = {
        { "fopen", (void *)hermes_fopen, &orig_fopen_handle },
        { "fclose", (void *)hermes_fclose, &orig_fclose_handle },
        { "fwrite", (void *)hermes_fwrite, &orig_fwrite_handle },
        { "fread", (void *)hermes_fread, &orig_fread_handle },
        { "fseek", (void *)hermes_fseek, &orig_fseek_handle },
};

void init_gotcha_stdio();

#endif  // GOTCHA_STDIO_H_
