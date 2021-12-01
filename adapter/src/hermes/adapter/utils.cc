#include <sys/file.h>

using hermes::u8;
namespace fs = std::experimental::filesystem;

void ReadGap(const std::string &filename, size_t seek_offset, u8 *read_ptr,
             size_t read_size, size_t file_bounds) {
  if (fs::exists(filename) &&
      fs::file_size(filename) >= file_bounds) {
    LOG(INFO) << "Blob has a gap in write. Read gap from original file.\n";
    INTERCEPTOR_LIST->hermes_flush_exclusion.insert(filename);
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd) {
      if (flock(fd, LOCK_SH) == -1) {
        hermes::FailedLibraryCall("flock");
      }

      ssize_t bytes_read = pread(fd, read_ptr, read_size, seek_offset);
      if (bytes_read == -1 || (size_t)bytes_read != read_size) {
        hermes::FailedLibraryCall("pread");
      }

      if (flock(fd, LOCK_UN) == -1) {
        hermes::FailedLibraryCall("flock");
      }

      if (close(fd) != 0) {
        hermes::FailedLibraryCall("close");
      }
    } else {
      hermes::FailedLibraryCall("open");
    }
    INTERCEPTOR_LIST->hermes_flush_exclusion.erase(filename);
  }
}
