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

#ifndef HERMES_ADAPTER_FACTORY_H
#define HERMES_ADAPTER_FACTORY_H

#include "abstract_adapter.h"
#include "singleton.h"

namespace hermes::adapter {
/**
 A class to represent adapter factory pattern
*/
class AdapterFactory {
 public:
  /**
   * Return the instance of adapter given a type. Uses factory pattern.
   *
   * @param[in] type type of mapper to be used by the POSIX adapter.
   * @return Instance of mapper given a type.
   */
  std::unique_ptr<AbstractAdapter> Get(const AdapterType& type) {
    switch (type) {
      case AdapterType::kPosix: {
        return HERMES_POSIX_FS;
      }
      case AdapterType::kStdio: {
        return nullptr;
      }
      case AdapterType::kMpiio: {
        return nullptr;
      }
      case AdapterType::kPubsub: {
        return nullptr;
      }
      case AdapterType::kVfd: {
        return nullptr;
      }
      default: {
        // TODO(llogan): @error_handling Mapper not implemented
      }
    }
    return NULL;
  }
};
}  // namespace hermes::adapter
#endif  // HERMES_ADAPTER_FACTORY_H
