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

#ifndef HERMES_DATA_PLACEMENT_ENGINE_H_
#define HERMES_DATA_PLACEMENT_ENGINE_H_

#include <map>

#include "hermes.h"
#include "status.h"
#include "hermes_types.h"
#include "metadata_manager.h"

namespace hermes {

using api::Status;
using hermes::api::PlacementPolicy;

/**
 A class to represent data placement engine
*/
class DPE {
 protected:
  MetadataManager *mdm_; /**< A pointer to the MDM */

 public:
  std::vector<f32> bandwidths; /**< a vector of bandwidths */

  /** Constructor. */
  DPE();

  /** Destructor. */
  virtual ~DPE() = default;

  /**
   * Calculate the placement of a set of blobs using a particular
   * algorithm given a context.
   * */
  virtual Status Placement(const std::vector<size_t> &blob_sizes,
                           const lipc::vector<TargetInfo> &targets,
                           const api::Context &ctx,
                           std::vector<PlacementSchema> &output) = 0;

  /**
   * Calculate the total placement for all blobs and output a schema for
   * each blob.
   * */
  Status CalculatePlacement(const std::vector<size_t> &blob_sizes,
                            std::vector<PlacementSchema> &output,
                            const api::Context &api_context);
};

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
