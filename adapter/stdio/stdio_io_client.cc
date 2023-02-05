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
  stat.fh_ = real_api->fopen(path.c_str(), stat.mode_str_.c_str());
  if (stat.fh_ == nullptr) {
    f.status_ = false;
    return;
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
  return real_api->fflush(stat.fh_);
}

/** Close \a file FILE f */
int StdioIoClient::RealClose(const IoClientObject &f,
                             IoClientStats &stat) {
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

/** Get initial statistics from the backend */
void StdioIoClient::InitBucketState(const lipc::charbuf &bkt_name,
                                    const IoClientContext &opts,
                                    GlobalIoClientState &stat) {
  // TODO(llogan)
}

/** Update backend statistics */
void StdioIoClient::UpdateBucketState(const IoClientContext &opts,
                                      GlobalIoClientState &stat) {
  stat.true_size_ = std::max(stat.true_size_,
                             opts.backend_off_ + opts.backend_size_);
}

/** Write blob to backend */
void StdioIoClient::WriteBlob(const lipc::charbuf &bkt_name,
                              const Blob &full_blob,
                              const IoClientContext &opts,
                              IoStatus &status) {
  std::string filename = bkt_name.str();
  LOG(INFO) << "Writing to file: " << filename
            << " offset: " << opts.backend_off_
            << " size:" << opts.backend_size_ << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r+");
  if (fh == nullptr) {
    status.size_ = 0;
    return;
  }
  real_api->fseek(fh, opts.backend_off_, SEEK_SET);
  status.size_ = real_api->fwrite(full_blob.data(),
                                       sizeof(char),
                                       full_blob.size(),
                                       fh);
  real_api->fclose(fh);
}

/** Read blob from the backend */
void StdioIoClient::ReadBlob(const lipc::charbuf &bkt_name,
                             Blob &full_blob,
                             const IoClientContext &opts,
                             IoStatus &status) {
  std::string filename = bkt_name.str();
  LOG(INFO) << "Read called for filename from destination: " << filename
            << " on offset: " << opts.backend_off_
            << " and size: " << opts.backend_size_ << "."
            << " file_size:" << stdfs::file_size(filename) << std::endl;
  FILE *fh = real_api->fopen(filename.c_str(), "r");
  if (fh == nullptr) {
    status.size_ = 0;
    return;
  }
  real_api->fseek(fh, opts.backend_off_, SEEK_SET);
  status.size_ = real_api->fread(full_blob.data(),
                                       sizeof(char),
                                       full_blob.size(),
                                       fh);
  real_api->fclose(fh);
}

}  // namespace hermes::adapter::fs
