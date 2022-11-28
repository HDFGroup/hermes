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
#include "hermes_status.h"
#include "hermes_types.h"

namespace hermes {

#define VERIFY_DPE_POLICY(ctx) \
  if (ctx.policy != policy_) { \
    return Status();           \
  }

using api::Status;
using hermes::api::PlacementPolicy;

/**
 A class to represent data placement engine
*/
class DPE {
 protected:
  PlacementPolicy policy_; /**< data placement policy */

 public:
  std::vector<f32> bandwidths; /**< a vector of bandwidths */
  /** constructor function */
  explicit DPE(PlacementPolicy policy) : policy_(policy) {}
  virtual ~DPE() = default;
  /**
     set placement schema given BLOB sizes, node states, targets, and context.
   */
  virtual Status Placement(const std::vector<size_t> &blob_sizes,
                           const std::vector<u64> &node_state,
                           const std::vector<TargetID> &targets,
                           const api::Context &ctx,
                           std::vector<PlacementSchema> &output) = 0;

 protected:
  /** get valid choices for splitting BLOB. */
  std::vector<int> GetValidSplitChoices(size_t blob_size);
  bool SplitBlob(size_t blob_size);
  /** get split sizes for \a blob_size. */
  void GetSplitSizes(size_t blob_size, std::vector<size_t> &output);
};

PlacementSchema AggregateBlobSchema(PlacementSchema &schema);

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          const std::vector<size_t> &blob_size,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context);

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
