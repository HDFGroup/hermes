#ifndef HERMES_ADAPTER_STDIO_H
#define HERMES_ADAPTER_STDIO_H

#include <bucket.h>
#include <fcntl.h>
#include <hermes.h>
#include <hermes/adapter/interceptor.h>
#include <hermes/adapter/singleton.h>
#include <hermes/adapter/stdio/common/constants.h>
#include <hermes/adapter/stdio/common/datastructures.h>
#include <hermes/adapter/stdio/mapper/mapper_factory.h>
#include <hermes/adapter/stdio/metadata_manager.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <vbucket.h>

#include <experimental/filesystem>

/**
 * Function declarations
 */

HERMES_FORWARD_DECL(ftell, long int, (FILE * fp));
HERMES_FORWARD_DECL(fopen, FILE *, (const char *path, const char *mode));
HERMES_FORWARD_DECL(fopen64, FILE *, (const char *path, const char *mode));
HERMES_FORWARD_DECL(fdopen, FILE *, (int fd, const char *mode));
HERMES_FORWARD_DECL(freopen, FILE *,
                    (const char *path, const char *mode, FILE *stream));
HERMES_FORWARD_DECL(freopen64, FILE *,
                    (const char *path, const char *mode, FILE *stream));
HERMES_FORWARD_DECL(fclose, int, (FILE * fp));
HERMES_FORWARD_DECL(fflush, int, (FILE * fp));
HERMES_FORWARD_DECL(fwrite, size_t,
                    (const void *ptr, size_t size, size_t nmemb, FILE *stream));
HERMES_FORWARD_DECL(fputc, int, (int c, FILE *stream));
HERMES_FORWARD_DECL(putc, int, (int c, FILE *stream));
HERMES_FORWARD_DECL(fgetpos, int, (FILE * stream, fpos_t *pos));
HERMES_FORWARD_DECL(putw, int, (int w, FILE *stream));
HERMES_FORWARD_DECL(fputs, int, (const char *s, FILE *stream));
HERMES_FORWARD_DECL(fprintf, int, (FILE * stream, const char *format, ...));
HERMES_FORWARD_DECL(printf, int, (const char *format, ...));
HERMES_FORWARD_DECL(vfprintf, int,
                    (FILE * stream, const char *format, va_list));
HERMES_FORWARD_DECL(vprintf, int, (const char *format, va_list));
HERMES_FORWARD_DECL(fread, size_t,
                    (void *ptr, size_t size, size_t nmemb, FILE *stream));
HERMES_FORWARD_DECL(fgetc, int, (FILE * stream));
HERMES_FORWARD_DECL(getc, int, (FILE * stream));
HERMES_FORWARD_DECL(getw, int, (FILE * stream));
HERMES_FORWARD_DECL(_IO_getc, int, (FILE * stream));
HERMES_FORWARD_DECL(_IO_putc, int, (int, FILE *stream));
HERMES_FORWARD_DECL(fgets, char *, (char *s, int size, FILE *stream));
HERMES_FORWARD_DECL(fseek, int, (FILE * stream, long offset, int whence));
HERMES_FORWARD_DECL(fseeko, int, (FILE * stream, off_t offset, int whence));
HERMES_FORWARD_DECL(fseeko64, int, (FILE * stream, off64_t offset, int whence));
HERMES_FORWARD_DECL(fsetpos, int, (FILE * stream, const fpos_t *pos));
HERMES_FORWARD_DECL(fsetpos64, int, (FILE * stream, const fpos64_t *pos));
HERMES_FORWARD_DECL(rewind, void, (FILE * stream));

#endif  // HERMES_ADAPTER_STDIO_H
