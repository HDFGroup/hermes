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

#ifndef HERMES_SRC_DPE_RANDOM_H_
#define HERMES_SRC_DPE_RANDOM_H_

#include "data_placement_engine.h"
#include <cstdlib>
#include <ctime>

namespace hermes {
/**
 A class to represent data placement engine that places data randomly.
*/
class Random : public DPE {
 public:
  Random() {
    // TODO(llogan): make seed configurable
    std::srand(2989248848);
  }
  ~Random() = default;
  Status Placement(const std::vector<size_t> &blob_sizes,
                   std::vector<TargetInfo> &targets,
                   api::Context &ctx,
                   std::vector<PlacementSchema> &output) override;
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_RANDOM_H_
