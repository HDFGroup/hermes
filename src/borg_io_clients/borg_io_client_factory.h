//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_FACTORY_H
#define HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_FACTORY_H

#include "borg_io_client.h"
#include "metadata_types.h"
#include "borg_posix_client.h"
#include "borg_ram_client.h"

namespace hermes::borg {

/**
 A class to represent I/O Client Factory
*/
class BorgIoClientFactory {
 public:
  /**
   * Get the I/O api implementation
   * */
  static std::unique_ptr<BorgIoClient> Get(IoInterface type) {
    switch (type) {
      case IoInterface::kPosix: {
        return std::make_unique<PosixIoClient>();
      }
      case IoInterface::kRam:
      default: {
        return std::make_unique<RamIoClient>();
      }
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_FACTORY_H
