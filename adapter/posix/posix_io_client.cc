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
void PosixIoClient::RealOpen(IoClientContext &f,
                             IoClientStat &stat,
                             const std::string &path) {
  if (stat.flags_ & O_CREAT || stat.flags_ & O_TMPFILE) {
    stat.fd_ = real_api->open(path.c_str(), stat.flags_, stat.st_mode_);
  } else {
    stat.fd_ = real_api->open(path.c_str(), stat.flags_);
  }
  if (stat.fd_ < 0) {
    f.status_ = false;
  }
  if (stat.flags_ & O_APPEND) {
    stat.is_append_ = true;
  }
}

/**
 * Called after real open. Allocates the Hermes representation of
 * identifying file information, such as a hermes file descriptor
 * and hermes file handler. These are not the same as POSIX file
 * descriptor and STDIO file handler.
 * */
void PosixIoClient::HermesOpen(IoClientContext &f,
                               const IoClientStat &stat,
                               FilesystemIoClientContext &fs_mdm) {
}

/** Synchronize \a file FILE f */
int PosixIoClient::RealSync(const IoClientContext &f,
                            const IoClientStat &stat) {
  (void) f;
  return real_api->fsync(stat.fd_);
}

/** Close \a file FILE f */
int PosixIoClient::RealClose(const IoClientContext &f,
                             const IoClientStat &stat) {
  (void) f;
  return real_api->close(stat.fd_);
}

/** Get initial statistics from the backend */
void PosixIoClient::InitBucketState(const lipc::charbuf &bkt_name,
                                    const IoClientOptions &opts,
                                    GlobalIoClientState &stat) {
  // real_api->__fxstat();
  stat.true_size_ = 0;
}

/** Update backend statistics */
void PosixIoClient::UpdateBucketState(const IoClientOptions &opts,
                                      GlobalIoClientState &stat) {
  stat.true_size_;
}

/** Write blob to backend */
void PosixIoClient::WriteBlob(const Blob &full_blob,
                              const IoClientContext &io_ctx,
                              const IoClientOptions &opts,
                              IoStatus &status) {
  (void) opts;
  LOG(INFO) << "Writing to file: " << io_ctx.filename_
            << " offset: " << opts.backend_off_
            << " size:" << full_blob.size() << "."
            << " file_size:" << stdfs::file_size(io_ctx.filename_)
            << std::endl;
  int fd = real_api->open(io_ctx.filename_.c_str(), O_RDWR | O_CREAT);
  if (fd < 0) {
    status.posix_ret_ = 0;
    return;
  }
  status.posix_ret_ = real_api->pwrite(fd,
                                       full_blob.data(),
                                       full_blob.size(),
                                       opts.backend_off_);
  real_api->close(fd);
}

/** Read blob from the backend */
void PosixIoClient::ReadBlob(Blob &full_blob,
                             const IoClientContext &io_ctx,
                             const IoClientOptions &opts,
                             IoStatus &status) {
  (void) opts;
  LOG(INFO) << "Writing to file: " << io_ctx.filename_
            << " offset: " << opts.backend_off_
            << " size:" << full_blob.size() << "."
            << " file_size:" << stdfs::file_size(io_ctx.filename_)
            << std::endl;
  int fd = real_api->open(io_ctx.filename_.c_str(), O_RDONLY);
  if (fd < 0) {
    status.posix_ret_ = 0;
    return;
  }
  status.posix_ret_ = real_api->pread(fd,
                                      full_blob.data(),
                                      full_blob.size(),
                                      opts.backend_off_);
  real_api->close(fd);
}

}  // namespace hermes::adapter::fs
