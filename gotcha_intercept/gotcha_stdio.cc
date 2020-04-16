extern "C" {
#include "gotcha_stdio.h"
}

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
  hermes_fopen_fp *orig_fopen = (hermes_fopen_fp *)gotcha_get_wrappee(orig_fopen_handle);
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

  hermes_fclose_fp *orig_fclose = (hermes_fclose_fp *)gotcha_get_wrappee(orig_fclose_handle);
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

  hermes_fwrite_fp *orig_fwrite = (hermes_fwrite_fp *)gotcha_get_wrappee(orig_fwrite_handle);
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

  hermes_fread_fp *orig_fread = (hermes_fread_fp *)gotcha_get_wrappee(orig_fread_handle);
  ret = orig_fread(ptr, size, count, stream);

  return ret; 
}

size_t hermes_fseek (FILE * stream, long int offset, int origin) {
  /** check if path matches prefix */
  bool path_match = 1;
  size_t ret = 0;

  printf("In wrapper hermes_fseek\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function fseek()\n");
    fflush(stdout);
  }

  hermes_fseek_fp *orig_fseek = (hermes_fseek_fp *)gotcha_get_wrappee(orig_fseek_handle);
  ret = orig_fseek(stream, offset, origin);
  
  return ret;
}

