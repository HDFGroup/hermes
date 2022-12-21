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

#ifndef HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_
#define HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_

#include "dpe/minimize_io_time.h"
#include "dpe/random.h"
#include "dpe/round_robin.h"

namespace hermes {

using hermes::api::PlacementPolicy;

/**
 A class to represent Data Placement Engine Factory
*/
class DPEFactory {
 public:
  /**
   * return a pointer to data placement engine given a policy type.
   * This uses factory pattern.
   *
   * @param[in] type a placement policy type to be used by the
   *            data placement engine factory.
   * @return pointer to DataPlacementEngine given \a type PlacementPolicy.
   */
  static std::unique_ptr<DPE> Get(const PlacementPolicy &type) {
    switch (type) {
      case PlacementPolicy::kRandom: {
        return std::make_unique<Random>();
      }
      case PlacementPolicy::kRoundRobin: {
        return std::make_unique<RoundRobin>();
      }
      case PlacementPolicy::kMinimizeIoTime: {
        return std::make_unique<MinimizeIoTime>();
      }
      case PlacementPolicy::kNone:
      default: {
        // TODO(luke): @errorhandling not implemented
        LOG(FATAL) << "PlacementPolicy not implemented" << std::endl;
        return NULL;
      }
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_
