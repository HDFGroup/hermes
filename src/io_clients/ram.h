//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_IO_CLIENTS_RAM_H_
#define HERMES_SRC_IO_CLIENTS_RAM_H_

#include "io_client.h"
#include "adapter/posix/real_api.h"
#include "adapter/posix/singleton_macros.h"

namespace hermes {

class RamIoClient : public IoClient {
 public:
  bool Init(DeviceInfo &dev_info) override {
  }

  bool Write(DeviceInfo &dev_info, void *data,
             size_t off, size_t size) override {
  }

  bool Read(DeviceInfo &dev_info, void *data,
            size_t off, size_t size) override {
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_IO_CLIENTS_RAM_H_
