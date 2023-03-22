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

#include "data_placement_engine_factory.h"

#include <assert.h>
#include <math.h>

#include <utility>
#include <random>
#include <map>

#include "hermes.h"
#include "hermes_types.h"
#include "buffer_pool.h"

namespace hermes {

/** Constructor. */
DPE::DPE() : mdm_(HERMES->mdm_.get()) {}

/** calculate data placement */
Status DPE::CalculatePlacement(const std::vector<size_t> &blob_sizes,
                               std::vector<PlacementSchema> &output,
                               api::Context &ctx) {
  AUTO_TRACE(1)
  Status result;

  // NOTE(chogan): Start with local targets and gradually expand the target
  // list until the placement succeeds.
  for (int i = 0; i < static_cast<int>(TopologyType::kCount); ++i) {
    // Reset the output schema
    output.clear();
    // Get the capacity/bandwidth of targets
    std::vector<TargetInfo> targets;
    switch (static_cast<TopologyType>(i)) {
      case TopologyType::Local: {
        targets = mdm_->LocalGetTargetInfo();
        break;
      }
      case TopologyType::Neighborhood: {
        // targets = mdm_->GetNeighborhoodTargetInfo();
        break;
      }
      case TopologyType::Global: {
        // targets = mdm_->GetGlobalTargetInfo();
        break;
      }
    }
    if (targets.size() == 0) {
      result = DPE_PLACEMENT_SCHEMA_EMPTY;
      continue;
    }
    // Calculate a placement schema
    result = Placement(blob_sizes, targets, ctx, output);
    if (!result.Fail()) {
      break;
    }
  }
  return result;
}

}  // namespace hermes
