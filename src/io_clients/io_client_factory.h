//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_IO_CLIENTS_IO_CLIENT_FACTORY_H_
#define HERMES_SRC_IO_CLIENTS_IO_CLIENT_FACTORY_H_

#include "io_client.h"
#include "metadata_types.h"
#include "posix.h"
#include "ram.h"

namespace hermes {

/**
 A class to represent Data Placement Engine Factory
*/
class IoClientFactory {
 public:
  /**
   * Get the I/O api corresponding to a particular device type
   * */
  static std::unique_ptr<IoClient> Get(IoInterface type) {
    switch (type) {
      case IoInterface::kPosix: {
        return std::make_unique<PosixIoClient>();
      }
      case IoInterface::kRam:
      default: {
        // TODO(llogan): @errorhandling not implemented
        LOG(FATAL) << "IoClient not implemented" << std::endl;
        return NULL;
      }
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_IO_CLIENTS_IO_CLIENT_FACTORY_H_
