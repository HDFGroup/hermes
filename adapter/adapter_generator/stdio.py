from adapter_generator.create_interceptor import Api,ApiClass

apis = [
    Api("int MPI_Init(int *argc, char ***argv)"),
    Api("int MPI_Finalize(void)"),
    Api("FILE *fopen(const char *path, const char *mode)"),
    Api("FILE *fopen64(const char *path, const char *mode)"),
    Api("FILE *fdopen(int fd, const char *mode)"),
    Api("FILE *freopen(const char *path, const char *mode, FILE *stream)"),
    Api("FILE *freopen64(const char *path, const char *mode, FILE *stream)"),
    Api("int fflush(FILE *fp)"),
    Api("int fclose(FILE *fp)"),
    Api("size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *fp)"),
    Api("int fputc(int c, FILE *fp)"),
    Api("int fgetpos(FILE *fp, fpos_t *pos)"),
    Api("int fgetpos64(FILE *fp, fpos64_t *pos)"),
    Api("int putc(int c, FILE *fp)"),
    Api("int putw(int w, FILE *fp)"),
    Api("int fputs(const char *s, FILE *stream)"),
    Api("size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream)"),
    Api("int fgetc(FILE *stream)"),
    Api("int getc(FILE *stream)"),
    Api("int getw(FILE *stream)"),
    Api("char *fgets(char *s, int size, FILE *stream)"),
    Api("void rewind(FILE *stream)"),
    Api("int fseek(FILE *stream, long offset, int whence)"),
    Api("int fseeko(FILE *stream, off_t offset, int whence)"),
    Api("int fseeko64(FILE *stream, off64_t offset, int whence)"),
    Api("int fsetpos(FILE *stream, const fpos_t *pos)"),
    Api("int fsetpos64(FILE *stream, const fpos64_t *pos)"),
    Api("long int ftell(FILE *fp)"),
]

includes = [
    "\"cstdio.h\""
]

ApiClass("stdio", apis, includes)