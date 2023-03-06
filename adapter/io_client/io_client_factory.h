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

#ifndef HERMES_IO_CLIENT_FACTORY_H
#define HERMES_IO_CLIENT_FACTORY_H

#include "io_client.h"
#include "hermes_shm/util/singleton.h"

#include "adapter/posix/posix_io_client.h"

namespace hermes::adapter {
/**
 A class to represent adapter factory pattern
*/
class IoClientFactory {
 public:
  /**
   * Return the instance of adapter given a type. Uses factory pattern.
   *
   * @param[in] type type of adapter.
   * @return Instance of mapper given a type.
   */
  static IoClient* Get(const AdapterType& type) {
    switch (type) {
      case AdapterType::kPosix: {
        return HERMES_POSIX_IO_CLIENT;
      }
      case AdapterType::kStdio: {
        return nullptr;
      }
      case AdapterType::kMpiio: {
        return nullptr;
      }
      case AdapterType::kPubsub:
      case AdapterType::kVfd:
      default: {
        return nullptr;
      }
    }
  }
};
}  // namespace hermes::adapter
#endif  // HERMES_IO_CLIENT_FACTORY_H
