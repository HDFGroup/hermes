//
// Created by lukemartinlogan on 9/9/22.
//

#ifndef HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_
#define HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_

#include "dpe/random.h"
#include "dpe/minimize_io_time.h"
#include "dpe/round_robin.h"
#include "data_placement_engine.h"

namespace hermes {

using namespace hermes::api;

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
        return std::make_unique<Random>();
      }
      case PlacementPolicy::kRoundRobin: {
        return std::make_unique<RoundRobin>();
      }
      case PlacementPolicy::kMinimizeIoTime: {
        return std::make_unique<MinimizeIoTime>();
      }
      default: {
        // TODO(luke): @errorhandling not implemented
        return NULL;
      }
    }
  }
};

}   // namespace hermes

#endif  // HERMES_SRC_DPE_DATA_PLACEMENT_ENGINE_FACTORY_H_
