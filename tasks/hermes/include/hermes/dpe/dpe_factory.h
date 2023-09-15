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

#include "minimize_io_time.h"
#include "random.h"
#include "round_robin.h"
#include "dpe.h"
#include "hermes/hermes.h"

namespace hermes {

/**
 A class to represent Data Placement Engine Factory
*/
class DpeFactory {
 public:
  /**
   * return a pointer to data placement engine given a policy type.
   * This uses factory pattern.
   *
   * @param[in] type a placement policy type to be used by the
   *            data placement engine factory.
   * @return pointer to DataPlacementEngine given \a type PlacementPolicy.
   */
  static Dpe* Get(PlacementPolicy type) {
    if (type == PlacementPolicy::kNone) {
      type = HERMES_SERVER_CONF.dpe_.default_policy_;
    }
    switch (type) {
      case PlacementPolicy::kRandom: {
        return hshm::EasySingleton<Random>::GetInstance();
      }
      case PlacementPolicy::kRoundRobin: {
        return hshm::EasySingleton<RoundRobin>::GetInstance();
      }
      case PlacementPolicy::kMinimizeIoTime: {
        return hshm::EasySingleton<MinimizeIoTime>::GetInstance();
      }
      case PlacementPolicy::kNone:
      default: {
        HELOG(kFatal, "PlacementPolicy not implemented")
        return NULL;
      }
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_
