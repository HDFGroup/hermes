/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYING file, which can be found at the top directory. If you do not  *
* have access to the file, you may request a copy from help@hdfgroup.org.   *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <fcntl.h>
#include "fs_api.h"
#include "real_api.h"

#include "filesystem/metadata_manager.cc"
#include "filesystem/filesystem.cc"

namespace hermes::adapter::posix {

File PosixFS::_RealOpen(AdapterStat &stat, const std::string &path) {
  File f;
  if (stat.flags & O_CREAT || stat.flags & O_TMPFILE) {
    f.fd_ = real_api->open(path.c_str(), stat.flags, stat.st_mode);
  } else {
    f.fd_ = real_api->open(path.c_str(), stat.flags);
  }
  if (f.fd_ < 0) {
    f.status_ = false;
  }
  _InitFile(f);
  return f;
}

void PosixFS::_InitFile(File &f) {
  struct stat st;
  real_api->__fxstat(_STAT_VER, f.fd_, &st);
  f.st_dev = st.st_dev;
  f.st_ino = st.st_ino;
}

void PosixFS::_OpenInitStats(File &f, AdapterStat &stat, bool bucket_exists) {
  (void) bucket_exists;
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
  /*if (bucket_exists) {
    stat.st_size = stat.st_bkid->GetTotalBlobSize();
    LOG(INFO) << "Since bucket exists, should reset its size to: " << stat.st_size
              << std::endl;
  }*/
  if (stat.flags & O_APPEND) {
    stat.st_ptr = stat.st_size;
    stat.is_append = true;
  }
}

size_t PosixFS::_RealWrite(const std::string &filename, off_t offset,
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

size_t PosixFS::_RealRead(const std::string &filename, off_t offset,
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

int PosixFS::_RealSync(File &f) {
  return real_api->fsync(f.fd_);
}

int PosixFS::_RealClose(File &f) {
  return real_api->close(f.fd_);
}

}  // namespace hermes::adapter::posix
