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

#include "posix_io_client.h"

namespace hermes::adapter::fs {

/** Allocate an fd for the file f */
void PosixIoClient::RealOpen(IoClientObject &f,
                             IoClientStats &stat,
                             const std::string &path) {
  if (stat.flags_ & O_CREAT || stat.flags_ & O_TMPFILE) {
    if (stat.adapter_mode_ != AdapterMode::kScratch) {
      stat.fd_ = real_api->open(path.c_str(), stat.flags_, stat.st_mode_);
    }
  } else {
    stat.fd_ = real_api->open(path.c_str(), stat.flags_);
  }
  if (stat.fd_ < 0) {
    f.status_ = false;
  }
  if (stat.flags_ & O_APPEND) {
    stat.is_append_ = true;
  }
  if (stat.flags_ & O_TRUNC) {
    stat.is_trunc_ = true;
  }
}

/**
 * Called after real open. Allocates the Hermes representation of
 * identifying file information, such as a hermes file descriptor
 * and hermes file handler. These are not the same as POSIX file
 * descriptor and STDIO file handler.
 * */
void PosixIoClient::HermesOpen(IoClientObject &f,
                               const IoClientStats &stat,
                               FilesystemIoClientObject &fs_mdm) {
  f.hermes_fd_ = fs_mdm.mdm_->AllocateFd();
}

/** Synchronize \a file FILE f */
int PosixIoClient::RealSync(const IoClientObject &f,
                            const IoClientStats &stat) {
  (void) f;
  return real_api->fsync(stat.fd_);
}

/** Close \a file FILE f */
int PosixIoClient::RealClose(const IoClientObject &f,
                             IoClientStats &stat) {
  (void) f;
  return real_api->close(stat.fd_);
}

/**
 * Called before RealClose. Releases information provisioned during
 * the allocation phase.
 * */
void PosixIoClient::HermesClose(IoClientObject &f,
                               const IoClientStats &stat,
                               FilesystemIoClientObject &fs_mdm) {
  fs_mdm.mdm_->ReleaseFd(f.hermes_fd_);
}

/** Get initial statistics from the backend */
void PosixIoClient::InitBucketState(const hipc::charbuf &bkt_name,
                                    const IoClientContext &opts,
                                    GlobalIoClientState &stat) {
  stat.true_size_ = 0;
  std::string filename = bkt_name.str();
  int fd = real_api->open(filename.c_str(), O_RDONLY);
  if (fd < 0) { return; }
  struct stat buf;
  real_api->fstat(fd, &buf);
  stat.true_size_ = buf.st_size;
  real_api->close(fd);

  LOG(INFO) << "The size of the file "
            << filename << " on disk is " << stat.true_size_ << std::endl;
}

/** Write blob to backend */
void PosixIoClient::WriteBlob(const hipc::charbuf &bkt_name,
                              const Blob &full_blob,
                              const IoClientContext &opts,
                              IoStatus &status) {
  (void) opts;
  status.success_ = true;
  LOG(INFO) << "Writing to file: " << bkt_name.str()
            << " offset: " << opts.backend_off_
            << " size:" << full_blob.size() << "."
            << " file_size:" << stdfs::file_size(bkt_name.str())
            << std::endl;
  int fd = real_api->open(bkt_name.str().c_str(), O_RDWR | O_CREAT);
  if (fd < 0) {
    status.size_ = 0;
    status.success_ = false;
    return;
  }
  status.size_ = real_api->pwrite(fd,
                                  full_blob.data(),
                                  full_blob.size(),
                                  opts.backend_off_);
  if (status.size_ != full_blob.size()) {
    status.success_ = false;
  }
  real_api->close(fd);
}

/** Read blob from the backend */
void PosixIoClient::ReadBlob(const hipc::charbuf &bkt_name,
                             Blob &full_blob,
                             const IoClientContext &opts,
                             IoStatus &status) {
  (void) opts;
  status.success_ = true;
  LOG(INFO) << "Reading from file: " << bkt_name.str()
            << " offset: " << opts.backend_off_
            << " size:" << full_blob.size() << "."
            << " file_size:" << stdfs::file_size(bkt_name.str())
            << std::endl;
  int fd = real_api->open(bkt_name.str().c_str(), O_RDONLY);
  if (fd < 0) {
    status.size_ = 0;
    status.success_ = false;
    return;
  }
  status.size_ = real_api->pread(fd,
                                 full_blob.data(),
                                 full_blob.size(),
                                 opts.backend_off_);
  if (status.size_ != full_blob.size()) {
    status.success_ = false;
  }
  real_api->close(fd);
}

}  // namespace hermes::adapter::fs
