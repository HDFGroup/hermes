//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_IO_CLIENTS_POSIX_H_
#define HERMES_SRC_IO_CLIENTS_POSIX_H_

#include "io_client.h"
#include "adapter/posix/real_api.h"
#include "adapter/posix/singleton_macros.h"

#include <experimental/filesystem>

namespace stdfs = std::experimental::filesystem;

namespace hermes {

class PosixIoClient : public IoClient {
 public:
  bool Init(DeviceInfo &dev_info) override {
    auto api = HERMES_POSIX_API;
    (*dev_info.mount_point_) = (*dev_info.mount_dir_) +
                            "/" + "slab_" + (*dev_info.dev_name_);
    int fd = api->open((*dev_info.mount_point_).c_str(),
                       O_TRUNC | O_CREAT, 0666);
    if (fd < 0) { return false; }
    api->close(fd);
    return true;
  }

  bool Write(DeviceInfo &dev_info, void *data,
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

  bool Read(DeviceInfo &dev_info, void *data,
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

#endif  // HERMES_SRC_IO_CLIENTS_POSIX_H_
