/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYFILE, which can be found at the top directory. If you do not have *
* access to either file, you may request a copy from help@hdfgroup.org.     *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_ADAPTER_FACTORY_H
#define HERMES_ADAPTER_FACTORY_H

/**
 * Standard header
 */

/**
 * Dependent library header
 */

/**
 * Internal header
 */
#include <hermes/adapter/singleton.h>
#include <hermes/adapter/stdio/common/enumerations.h>

#include "abstract_mapper.h"
#include "balanced_mapper.h"

namespace hermes::adapter::stdio {
class MapperFactory {
 public:
  /**
   * Return the instance of mapper given a type. Uses factory pattern.
   *
   * @param type, MapperType, type of mapper to be used by the STDIO adapter.
   * @return Instance of mapper given a type.
   */
  std::shared_ptr<AbstractMapper> Get(const MapperType &type) {
    switch (type) {
      case MapperType::BALANCED: {
        return hermes::adapter::Singleton<BalancedMapper>::GetInstance();
      }
      default: {
        // TODO(hari): @error_handling Mapper not implemented
      }
    }
    return NULL;
  }
};
}  // namespace hermes::adapter::stdio
#endif  // HERMES_ADAPTER_FACTORY_H
