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
