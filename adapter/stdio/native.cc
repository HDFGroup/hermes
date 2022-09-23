//
// Created by lukemartinlogan on 9/22/22.
//


#include <fcntl.h>
#include "stdio.h"
#include "native.h"

namespace hermes::adapter::stdio {

File StdioFS::_RealOpen(AdapterStat &stat, const std::string &path) {
  File f;
  f.fh_ = real_api->fopen(path.c_str(), stat.mode_str.c_str());
  if (f.fh_ == nullptr) {
    f.status_ = false;
  }
  _InitFile(f);
  return f;
}

void StdioFS::_InitFile(File &f) {
  struct stat st;
  f.fd_ = fileno(f.fh_);
  posix_api->__fxstat(_STAT_VER, f.fd_, &st);
  f.st_dev = st.st_dev;
  f.st_ino = st.st_ino;
}

void StdioFS::_OpenInitStats(File &f, AdapterStat &stat, bool bucket_exists) {
  (void) bucket_exists;
  struct stat st;
  posix_api->__fxstat(_STAT_VER, f.fd_, &st);
  stat.st_mode = st.st_mode;
  stat.st_uid = st.st_uid;
  stat.st_gid = st.st_gid;
  stat.st_size = st.st_size;
  std::string fn = GetFilenameFromFD(f.fd_);
  /*if (fn.find("/tmp/#") == std::string::npos) {
    LOG(INFO) << "fd: " << f.fd_
              << " fxstat size: " << stat.st_size
              << " stdfs size: " << stdfs::file_size(GetFilenameFromFD(f.fd_))
              << std::endl;
  }*/
  stat.st_blksize = st.st_blksize;
  stat.st_atim = st.st_atim;
  stat.st_mtim = st.st_mtim;
  stat.st_ctim = st.st_ctim;
  /*if (bucket_exists) {
    stat.st_size = stat.st_bkid->GetTotalBlobSize();
    LOG(INFO) << "Since bucket exists, should reset its size to: " << stat.st_size
              << std::endl;
  }*/
  if (stat.flags & O_APPEND) {
    stat.st_ptr = stat.st_size;
  }
}

size_t StdioFS::_RealWrite(const std::string &filename, off_t offset,
                           size_t size, u8 *data_ptr) {
  LOG(INFO) << "Writing to file: " << filename
            << " offset: " << offset
            << " size:" << size << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r+");
  if (fh == nullptr) { return 0; }
  size_t write_size = 0;
  if (real_api->fseek(fh, offset, SEEK_SET) != 0) { return 0; }
  write_size = real_api->fwrite(data_ptr, sizeof(char), size, fh);
  real_api->fclose(fh);
  return write_size;
}

size_t StdioFS::_RealRead(const std::string &filename, off_t offset,
                          size_t size, u8 *data_ptr) {
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << offset
            << " and size: " << size << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r");
  if (fh == nullptr) { return 0; }
  size_t read_size = 0;
  real_api->fseek(fh, offset, SEEK_SET);
  read_size = real_api->fread((char *)data_ptr + offset, sizeof(char), size, fh);
  real_api->fclose(fh);
  return read_size;
}

int StdioFS::_RealSync(File &f) {
  return real_api->fflush(f.fh_);
}

int StdioFS::_RealClose(File &f) {
  return real_api->fclose(f.fh_);
}

}  // namespace hermes::adapter::stdio
