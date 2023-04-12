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

#ifndef HERMES_SRC_BORG_IO_CLIENTS_POSIX_H
#define HERMES_SRC_BORG_IO_CLIENTS_POSIX_H

#include "borg_io_client.h"
#include "io_client/posix/posix_api.h"

#include <filesystem>

namespace stdfs = std::filesystem;

namespace hermes::borg {

class PosixIoClient : public BorgIoClient {
 public:
  virtual ~PosixIoClient() = default;

  bool Init(DeviceInfo &dev_info) override {
    auto api = HERMES_POSIX_API;
    std::string text = (*dev_info.mount_dir_).str() +
                        "/" + "slab_" + (*dev_info.dev_name_).str();
    auto canon = stdfs::weakly_canonical(text).string();
    (*dev_info.mount_point_) = canon;
    int fd = api->open((*dev_info.mount_point_).c_str(),
                       O_TRUNC | O_CREAT, 0666);
    if (fd < 0) { return false; }
    api->close(fd);
    return true;
  }

  bool Write(DeviceInfo &dev_info, const char *data,
             size_t off, size_t size) override {
    auto api = HERMES_POSIX_API;
    auto mount_point = (*dev_info.mount_point_).str();
    int fd = api->open(mount_point.c_str(), O_RDWR);
    if (fd < 0) {
      HELOG(kError, "Failed to open (write): {}",
            dev_info.mount_point_->str())
      return false;
    }
    size_t count = api->pwrite(fd, data, size, off);
    api->close(fd);
    if (count != size) {
      HELOG(kError, "BORG: wrote {} bytes, but expected {}",
            count, size);
      return false;
    }
    return true;
  }

  bool Read(DeviceInfo &dev_info, char *data,
            size_t off, size_t size) override {
    auto api = HERMES_POSIX_API;
    auto mount_point = (*dev_info.mount_point_).str();
    int fd = api->open(mount_point.c_str(), O_RDWR);
    if (fd < 0) {
      HELOG(kError, "Failed to open (read): {}",
            dev_info.mount_point_->str())
      return false;
    }
    size_t count = api->pread(fd, data, size, off);
    api->close(fd);
    if (count != size) {
      HELOG(kError, "BORG: read {} bytes, but expected {}",
            count, size);
      return false;
    }
    return true;
  }
};

}  // namespace hermes::borg

#endif  // HERMES_SRC_BORG_IO_CLIENTS_POSIX_H
