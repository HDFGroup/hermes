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
void PosixIoClient::RealOpen(File &f,
                             AdapterStat &stat,
                             const std::string &path) {
  if (stat.adapter_mode_ == AdapterMode::kScratch) {
    stat.flags_ &= ~O_CREAT;
  }
  if (stat.flags_ & O_CREAT || stat.flags_ & O_TMPFILE) {
    if (stat.adapter_mode_ != AdapterMode::kScratch) {
      stat.fd_ = real_api->open(path.c_str(), stat.flags_, stat.st_mode_);
    }
  } else {
    stat.fd_ = real_api->open(path.c_str(), stat.flags_);
  }
  if (stat.adapter_mode_ != AdapterMode::kScratch &&
      stat.fd_ < 0) {
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
void PosixIoClient::HermesOpen(File &f,
                               const AdapterStat &stat,
                               FilesystemIoClientState &fs_mdm) {
  f.hermes_fd_ = fs_mdm.mdm_->AllocateFd();
}

/** Synchronize \a file FILE f */
int PosixIoClient::RealSync(const File &f,
                            const AdapterStat &stat) {
  (void) f;
  if (stat.adapter_mode_ == AdapterMode::kScratch &&
      stat.fd_ == -1) {
    return 0;
  }
  return real_api->fsync(stat.fd_);
}

/** Close \a file FILE f */
int PosixIoClient::RealClose(const File &f,
                             AdapterStat &stat) {
  (void) f;
  if (stat.adapter_mode_ == AdapterMode::kScratch &&
      stat.fd_ == -1) {
    return 0;
  }
  return real_api->close(stat.fd_);
}

/**
 * Called before RealClose. Releases information provisioned during
 * the allocation phase.
 * */
void PosixIoClient::HermesClose(File &f,
                               const AdapterStat &stat,
                               FilesystemIoClientState &fs_mdm) {
  fs_mdm.mdm_->ReleaseFd(f.hermes_fd_);
}

/** Remo
 * ve \a file FILE f */
int PosixIoClient::RealRemove(const std::string &path) {
  return real_api->remove(path.c_str());
}

/** Get initial statistics from the backend */
size_t PosixIoClient::GetSize(const hipc::charbuf &bkt_name) {
  size_t true_size = 0;
  std::string filename = bkt_name.str();
  int fd = real_api->open(filename.c_str(), O_RDONLY);
  if (fd < 0) { return 0; }
  struct stat buf;
  real_api->fstat(fd, &buf);
  true_size = buf.st_size;
  real_api->close(fd);

  HILOG(kDebug, "The size of the file {} on disk is {}",
        filename, true_size)
  return true_size;
}

/** Write blob to backend */
void PosixIoClient::WriteBlob(const hipc::charbuf &bkt_name,
                              const Blob &full_blob,
                              const FsIoOptions &opts,
                              IoStatus &status) {
  (void) opts;
  status.success_ = true;
  HILOG(kDebug, "Writing to file: {}"
        " offset: {}"
        " size: {}"
        " file_size: {}",
        bkt_name.str(), opts.backend_off_, full_blob.size(),
        stdfs::file_size(bkt_name.str()))
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
                             const FsIoOptions &opts,
                             IoStatus &status) {
  (void) opts;
  status.success_ = true;
  HILOG(kDebug, "Reading from file: {}"
        " offset: {}"
        " size: {}"
        " file_size: {}",
        bkt_name.str(), opts.backend_off_, full_blob.size(),
        stdfs::file_size(bkt_name.str()))
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
