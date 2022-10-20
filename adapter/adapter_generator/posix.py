from adapter_generator.create_interceptor import Api,ApiClass

apis = [
    Api("int MPI_Init(int *argc, char ***argv)"),
    Api("int MPI_Finalize(void)"),
    Api("int open(const char *path, int flags, ...)"),
    Api("int open64(const char *path, int flags, ...)"),
    Api("int __open_2(const char *path, int oflag)"),
    Api("int creat(const char *path, mode_t mode)"),
    Api("int creat64(const char *path, mode_t mode)"),
    Api("ssize_t read(int fd, void *buf, size_t count)"),
    Api("ssize_t write(int fd, const void *buf, size_t count)"),
    Api("ssize_t pread(int fd, void *buf, size_t count, off_t offset)"),
    Api("ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset)"),
    Api("ssize_t pread64(int fd, void *buf, size_t count, off64_t offset)"),
    Api("ssize_t pwrite64(int fd, const void *buf, size_t count, off64_t offset)"),
    Api("off_t lseek(int fd, off_t offset, int whence)"),
    Api("off64_t lseek64(int fd, off64_t offset, int whence)"),
    Api("int fstat(int fd, struct stat *buf)"),
    Api("int fsync(int fd)"),
    Api("int close(int fd)"),
]

includes = [
    "<unistd.h>",
    "<fcntl.h>",
    "\"interceptor.h\"",
    "\"filesystem/filesystem.h\"",
    "\"filesystem/metadata_manager.h\""
]

ApiClass("posix", apis, includes)