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
  if (fd < 0) { return 0; }
  size_t write_size = real_api->pwrite(fd, data_ptr, size, offset);
  real_api->close(fd);
  return write_size;
}

size_t PosixFS::_RealRead(const std::string &filename, off_t offset, size_t size,
                          u8 *data_ptr) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << offset << " and size: " << size
            << std::endl;
  int fd = real_api->open(filename.c_str(), O_RDONLY);
  if (fd < 0) { return 0; }
  size_t read_size = real_api->pread(fd, data_ptr, size, offset);
  real_api->close(fd);
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

void PosixFS::_OpenInitStats(File &f, AdapterStat &stat, bool bucket_exists) {
  struct stat st;
  real_api->__fxstat(_STAT_VER, f.fd_, &st);
  stat.st_mode = st.st_mode;
  stat.st_uid = st.st_uid;
  stat.st_gid = st.st_gid;
  stat.st_size = st.st_size;
  stat.st_blksize = st.st_blksize;
  stat.st_atim = st.st_atim;
  stat.st_mtim = st.st_mtim;
  stat.st_ctim = st.st_ctim;
  if (bucket_exists) {
    stat.st_size = stat.st_bkid->GetTotalBlobSize();
  }
  if (stat.flags & O_APPEND) {
    stat.st_ptr = stat.st_size;
  }
}

}  // namespace hermes::adapter::posix