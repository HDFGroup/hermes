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
#include "adapter/posix/posix_api.h"
#include "adapter/posix/posix_api_singleton_macros.h"

#include <experimental/filesystem>

namespace stdfs = std::experimental::filesystem;

namespace hermes::borg {

class PosixIoClient : public BorgIoClient {
 public:
  bool Init(DeviceInfo &dev_info) override {
    auto api = HERMES_POSIX_API;
    lipc::string text = (*dev_info.mount_dir_) +
                        "/" + "slab_" + (*dev_info.dev_name_);
    (*dev_info.mount_point_) = std::move(text);
    int fd = api->open((*dev_info.mount_point_).c_str(),
                       O_TRUNC | O_CREAT, 0666);
    if (fd < 0) { return false; }
    api->close(fd);
    return true;
  }

  bool Write(DeviceInfo &dev_info, const char *data,
             size_t off, size_t size) override {
    auto api = HERMES_POSIX_API;
    int fd = api->open((*dev_info.mount_point_).c_str(), O_RDWR);
    if (fd < 0) {
      LOG(INFO) << "Failed to open (write): "
                << dev_info.mount_point_->str() << std::endl;
      return false;
    }
    size_t count = api->pwrite(fd, data, size, off);
    api->close(fd);
    return count == size;
  }

  bool Read(DeviceInfo &dev_info, char *data,
            size_t off, size_t size) override {
    auto api = HERMES_POSIX_API;
    int fd = api->open((*dev_info.mount_point_).c_str(), O_RDWR);
    if (fd < 0) {
      LOG(INFO) << "Failed to open (read): "
                << dev_info.mount_point_->str() << std::endl;
      return false;
    }
    size_t count = api->pread(fd, data, size, off);
    api->close(fd);
    return count == size;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_BORG_IO_CLIENTS_POSIX_H
