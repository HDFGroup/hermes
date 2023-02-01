//
// Created by lukemartinlogan on 1/31/23.
//

#include "posix_io_client.h"

namespace hermes::adapter::posix {

IoClientContext PosixIoClient::_RealOpen(IoClientStat &stat,
                                         const std::string &path) {
  IoClientContext f;
  if (stat.flags_ & O_CREAT || stat.flags_ & O_TMPFILE) {
    f.fd_ = real_api->open(path.c_str(), stat.flags_, stat.st_mode_);
  } else {
    f.fd_ = real_api->open(path.c_str(), stat.flags_);
  }
  if (f.fd_ < 0) {
    f.status_ = false;
  }
  _InitFile(f);
  return f;
}

void PosixIoClient::_InitFile(IoClientContext &f) {
  struct stat st;
  real_api->__fxstat(_STAT_VER, f.fd_, &st);
  f.st_dev = st.st_dev;
  f.st_ino = st.st_ino;
}

void PosixIoClient::_OpenInitStats(IoClientContext &f, IoClientStat &stat) {
  struct stat st;
  real_api->__fxstat(_STAT_VER, f.fd_, &st);
  stat.st_mode_ = st.st_mode;
  stat.st_uid_ = st.st_uid;
  stat.st_gid_ = st.st_gid;
  // stat.st_size_ = st.st_size;
  // stat.st_blksize_ = st.st_blksize;
  stat.st_atim_ = st.st_atim;
  stat.st_mtim_ = st.st_mtim;
  stat.st_ctim_ = st.st_ctim;
  if (stat.flags_ & O_APPEND) {
    stat.is_append_ = true;
  }
}

size_t PosixIoClient::WriteBlob(const std::string &filename, off_t offset,
                           size_t size, const u8 *data_ptr,
                           IoStatus &io_status, IoOptions &opts) {
  (void) opts; (void) io_status;
  LOG(INFO) << "Writing to file: " << filename
            << " offset: " << offset
            << " size:" << size << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  int fd = real_api->open(filename.c_str(), O_RDWR | O_CREAT);
  if (fd < 0) { return 0; }
  size_t write_size = real_api->pwrite(fd, data_ptr, size, offset);
  real_api->close(fd);
  return write_size;
}

size_t PosixIoClient::ReadBlob(const std::string &filename, off_t offset,
                          size_t size, u8 *data_ptr,
                          IoStatus &io_status, IoOptions &opts) {
  (void) opts; (void) io_status;
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << offset
            << " and size: " << size << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  int fd = real_api->open(filename.c_str(), O_RDONLY);
  if (fd < 0) { return 0; }
  size_t read_size = real_api->pread(fd, data_ptr, size, offset);
  real_api->close(fd);
  return read_size;
}

int PosixIoClient::_RealSync(IoClientContext &f) {
  return real_api->fsync(f.fd_);
}

int PosixIoClient::_RealClose(IoClientContext &f) {
  return real_api->close(f.fd_);
}

}  // namespace hermes::adapter::posix
