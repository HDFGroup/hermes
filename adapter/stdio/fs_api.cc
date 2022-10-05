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
#include "real_api.h"
#include "fs_api.h"
#include <sys/file.h>

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
  if (f.fh_ == nullptr) {
    f.fd_ = -1;
    return;
  }
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
  stat.st_blksize = st.st_blksize;
  stat.st_atim = st.st_atim;
  stat.st_mtim = st.st_mtim;
  stat.st_ctim = st.st_ctim;
  /*if (bucket_exists) {
    stat.st_size = stat.st_bkid->GetTotalBlobSize();
    LOG(INFO) << "Since bucket exists, should reset its size to: " << stat.st_size
              << std::endl;
  }*/
  if (stat.mode_str.find('a') != std::string::npos) {
    stat.st_ptr = stat.st_size;
    stat.is_append = true;
  }
}

size_t StdioFS::_RealWrite(const std::string &filename, off_t offset,
                           size_t size, const u8 *data_ptr, IoOptions &opts) {
  (void) opts;
  LOG(INFO) << "Writing to file: " << filename
            << " offset: " << offset
            << " size:" << size << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r+");
  if (fh == nullptr) { return 0; }
  real_api->fseek(fh, offset, SEEK_SET);
  flock(fileno(fh), LOCK_EX);
  size_t write_size = real_api->fwrite(data_ptr, sizeof(char), size, fh);
  flock(fileno(fh), LOCK_UN);
  real_api->fclose(fh);
  return write_size;
}

size_t StdioFS::_RealRead(const std::string &filename, off_t offset,
                          size_t size, u8 *data_ptr, IoOptions &opts) {
  (void) opts;
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << offset
            << " and size: " << size << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r");
  if (fh == nullptr) { return 0; }
  real_api->fseek(fh, offset, SEEK_SET);
  flock(fileno(fh), LOCK_SH);
  size_t read_size = real_api->fread(data_ptr, sizeof(char), size, fh);
  flock(fileno(fh), LOCK_UN);
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
