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

#include "hermes_types.h"
#include "hermes_status.h"
#include "hermes.h"

namespace hermes {

using api::Status;

class DPE {
 protected:
  bool require_bw_;
 public:
  virtual Status Placement(const std::vector<size_t> &blob_sizes,
                           const std::vector<u64> &node_state,
                           const std::vector<f32> &bandwidths,
                           const std::vector<TargetID> &targets,
                           const api::Context &ctx,
                           std::vector<PlacementSchema> &output) = 0;
 protected:
  std::vector<int> GetValidSplitChoices(size_t blob_size);
  bool SplitBlob(size_t blob_size);
  void GetSplitSizes(size_t blob_size, std::vector<size_t> &output);
};

PlacementSchema AggregateBlobSchema(PlacementSchema &schema);

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          const std::vector<size_t> &blob_size,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context);

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
