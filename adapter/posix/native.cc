//
// Created by lukemartinlogan on 9/20/22.
//

#include <fcntl.h>
#include "native.h"
#include "posix.h"

namespace hermes::adapter::posix {

size_t PosixFS::_RealWrite(const std::string &filename, off_t offset,
                           size_t size, u8 *data_ptr) {
  LOG(INFO) << "Writing to file: " << filename << " offset: " << offset
            << " of size:" << size << "." << std::endl;
  int fd = real_api->open(filename.c_str(), O_RDWR | O_CREAT);
  if (fd < 0) {
    return 0;
  }
  size_t write_size = 0;
  long status = real_api->lseek(fd, offset, SEEK_SET);
  if (status == offset) {
    write_size = real_api->write(fd, data_ptr, size);
    status = real_api->close(fd);
  }
  return write_size;
}

size_t PosixFS::_RealRead(const char *filename, off_t file_offset, void *ptr,
                          size_t ptr_offset, size_t size) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << file_offset << " and size: " << size
            << std::endl;
  int fd = real_api->open(filename, O_RDONLY);
  if (fd < 0) {
    return 0;
  }

  size_t read_size = 0;
  int status = real_api->lseek(fd, file_offset, SEEK_SET);
  if (status == file_offset) {
    read_size = real_api->read(fd, (char *)ptr + ptr_offset, size);
  }
  status = real_api->close(fd);
  return read_size;
}

File PosixFS::_RealOpen(AdapterStat &stat, const std::string &path) {
  File ret;
  if (stat.flags & O_CREAT || stat.flags & O_TMPFILE) {
    ret.fd_ = real_api->open(path.c_str(), stat.flags, stat.st_mode);
  } else {
    ret.fd_ = real_api->open(path.c_str(), stat.flags);
  }
  if (ret.fd_ < 0) {
    ret.status_ = false;
  }
  return ret;
}

}  // namespace hermes::adapter::posix