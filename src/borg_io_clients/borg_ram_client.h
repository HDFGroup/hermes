//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_BORG_IO_CLIENTS_RAM_H_
#define HERMES_SRC_BORG_IO_CLIENTS_RAM_H_

#include "borg_io_client.h"
#include "adapter/posix/posix_api.h"
#include "adapter/posix/posix_singleton_macros.h"
#include "hermes.h"

namespace hermes::borg {

class RamIoClient : public BorgIoClient {
 public:
  bool Init(DeviceInfo &dev_info) override {
    auto &hermes_header = HERMES->header_;
    auto &main_alloc = HERMES->main_alloc_;
    auto &server_config = HERMES->server_config_;
    hermes_header->ram_tier_ = main_alloc->
                               Allocate(dev_info.header_->capacity_);
    if (hermes_header->ram_tier_.IsNull()) {
      LOG(FATAL) << BUFFER_POOL_OUT_OF_RAM.Msg() << std::endl;
    }
    return true;
  }

  bool Write(DeviceInfo &dev_info, const char *data,
             size_t off, size_t size) override {
    auto &hermes_header = HERMES->header_;
    auto &main_alloc = HERMES->main_alloc_;
    char *ram_ptr = main_alloc->Convert<char>(hermes_header->ram_tier_);
    memcpy(ram_ptr + off, data, size);
    return true;
  }

  bool Read(DeviceInfo &dev_info, char *data,
            size_t off, size_t size) override {
    auto &hermes_header = HERMES->header_;
    auto &main_alloc = HERMES->main_alloc_;
    char *ram_ptr = main_alloc->Convert<char>(hermes_header->ram_tier_);
    memcpy(data, ram_ptr + off, size);
    return true;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_BORG_IO_CLIENTS_RAM_H_
