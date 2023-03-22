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

#include "stdio_io_client.h"

namespace hermes::adapter::fs {

/** Allocate an fd for the file f */
void StdioIoClient::RealOpen(IoClientObject &f,
                             IoClientStats &stat,
                             const std::string &path) {
  if (stat.mode_str_.find('w') != std::string::npos) {
    stat.is_trunc_ = true;
  }
  if (!(stat.is_trunc_ && stat.adapter_mode_ == AdapterMode::kScratch)) {
    stat.fh_ = real_api->fopen(path.c_str(), stat.mode_str_.c_str());
    if (stat.fh_ == nullptr) {
      f.status_ = false;
      return;
    }
  }
  if (stat.mode_str_.find('a') != std::string::npos) {
    stat.is_append_ = true;
  }
}

/**
 * Called after real open. Allocates the Hermes representation of
 * identifying file information, such as a hermes file descriptor
 * and hermes file handler. These are not the same as POSIX file
 * descriptor and STDIO file handler.
 * */
void StdioIoClient::HermesOpen(IoClientObject &f,
                               const IoClientStats &stat,
                               FilesystemIoClientObject &fs_mdm) {
  f.hermes_fh_ = (FILE*)fs_mdm.stat_;
}

/** Synchronize \a file FILE f */
int StdioIoClient::RealSync(const IoClientObject &f,
                            const IoClientStats &stat) {
  (void) f;
  if (stat.adapter_mode_ == AdapterMode::kScratch &&
      stat.fh_ == nullptr) {
    return 0;
  }
  return real_api->fflush(stat.fh_);
}

/** Close \a file FILE f */
int StdioIoClient::RealClose(const IoClientObject &f,
                             IoClientStats &stat) {
  if (stat.adapter_mode_ == AdapterMode::kScratch &&
      stat.fh_ == nullptr) {
    return 0;
  }
  return real_api->fclose(stat.fh_);
}

/**
 * Called before RealClose. Releases information provisioned during
 * the allocation phase.
 * */
void StdioIoClient::HermesClose(IoClientObject &f,
                               const IoClientStats &stat,
                               FilesystemIoClientObject &fs_mdm) {
  (void) f; (void) stat; (void) fs_mdm;
}

/** Remove \a file FILE f */
int StdioIoClient::RealRemove(const std::string &path) {
  return remove(path.c_str());
}

/** Get initial statistics from the backend */
size_t StdioIoClient::GetSize(const hipc::charbuf &bkt_name) {
  size_t true_size = 0;
  std::string filename = bkt_name.str();
  int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0) { return 0; }
  struct stat buf;
  fstat(fd, &buf);
  true_size = buf.st_size;
  close(fd);

  LOG(INFO) << "The size of the file "
            << filename << " on disk is " << true_size << std::endl;
  return true_size;
}

/** Write blob to backend */
void StdioIoClient::WriteBlob(const hipc::charbuf &bkt_name,
                              const Blob &full_blob,
                              const IoClientContext &opts,
                              IoStatus &status) {
  status.success_ = true;
  std::string filename = bkt_name.str();
  LOG(INFO) << "Writing to file: " << filename
            << " offset: " << opts.backend_off_
            << " size:" << opts.backend_size_ << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r+");
  if (fh == nullptr) {
    status.size_ = 0;
    status.success_ = false;
    return;
  }
  real_api->fseek(fh, opts.backend_off_, SEEK_SET);
  status.size_ = real_api->fwrite(full_blob.data(),
                                       sizeof(char),
                                       full_blob.size(),
                                       fh);
  if (status.size_ != full_blob.size()) {
    status.success_ = false;
  }
  real_api->fclose(fh);
}

/** Read blob from the backend */
void StdioIoClient::ReadBlob(const hipc::charbuf &bkt_name,
                             Blob &full_blob,
                             const IoClientContext &opts,
                             IoStatus &status) {
  status.success_ = true;
  std::string filename = bkt_name.str();
  LOG(INFO) << "Reading from file: " << filename
            << " on offset: " << opts.backend_off_
            << " and size: " << opts.backend_size_ << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r");
  if (fh == nullptr) {
    status.size_ = 0;
    status.success_ = false;
    return;
  }
  real_api->fseek(fh, opts.backend_off_, SEEK_SET);
  status.size_ = real_api->fread(full_blob.data(),
                                 sizeof(char),
                                 full_blob.size(),
                                 fh);
  if (status.size_ != full_blob.size()) {
    status.success_ = false;
  }
  real_api->fclose(fh);
}

}  // namespace hermes::adapter::fs
