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

#include "dpe/random.h"
#include "dpe/minimize_io_time.h"
#include "dpe/round_robin.h"

namespace hermes {

using hermes::api::PlacementPolicy;

class DPEFactory {
 public:
  /**
   * Return the instance of mapper given a type. Uses factory pattern.
   *
   * @param type, MapperType, type of mapper to be used by the STDIO adapter.
   * @return Instance of mapper given a type.
   */
  std::unique_ptr<DPE> Get(const PlacementPolicy &type) {
    switch (type) {
      case PlacementPolicy::kRandom: {
        return std::unique_ptr<Random>(new Random());
      }
      case PlacementPolicy::kRoundRobin: {
        return std::unique_ptr<RoundRobin>(new RoundRobin());
      }
      case PlacementPolicy::kMinimizeIoTime: {
        return std::unique_ptr<MinimizeIoTime>(new MinimizeIoTime());
      }
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
