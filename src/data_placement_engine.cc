#include "data_placement_engine.h"

#include <assert.h>
#include <math.h>

#include <utility>

#include "hermes.h"
#include "metadata_management.h"

namespace hermes {

using hermes::api::Status;

enum class PlacementPolicy {
  kRandom,
  kTopDown,
};

// TODO(chogan): Unfinished sketch
Status TopDownPlacement(SharedMemoryContext *context, RpcContext *rpc,
                        std::vector<size_t> blob_sizes,
                        std::vector<TieredSchema> &output) {
  HERMES_NOT_IMPLEMENTED_YET;

  Status result = 0;
  std::vector<u64> global_state = GetGlobalTierCapacities(context, rpc);

  for (auto &blob_size : blob_sizes) {
    TieredSchema schema;
    size_t size_left = blob_size;
    TierID current_tier = 0;

    while (size_left > 0 && current_tier < global_state.size()) {
      size_t bytes_used = 0;
      if (global_state[current_tier] > size_left) {
        bytes_used = size_left;
      } else {
        bytes_used = global_state[current_tier];
        current_tier++;
      }

      if (bytes_used) {
        size_left -= bytes_used;
        schema.push_back(std::make_pair(current_tier, bytes_used));
      }
    }

    if (size_left > 0) {
      // TODO(chogan): Trigger BufferOrganizer
      // EvictBuffers(eviction_schema);
      schema.clear();
    }

    output.push_back(schema);
  }

  return result;
}

Status RandomPlacement(SharedMemoryContext *context, RpcContext *rpc,
                       std::vector<size_t> &blob_sizes,
                       std::vector<TieredSchema> &output) {
  std::vector<u64> global_state = GetGlobalTierCapacities(context, rpc);
  Status result = 0;
  for (auto &blob_size : blob_sizes) {
    TieredSchema schema;
    TierID tier_id = rand() % global_state.size();

    if (global_state[tier_id] > blob_size) {
      schema.push_back(std::make_pair(blob_size, tier_id));
    } else {
      HERMES_NOT_IMPLEMENTED_YET;
      // TODO(chogan): Trigger BufferOrganizer
      // EvictBuffers(eviction_schema);
    }
    output.push_back(schema);
  }

  return result;
}

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          std::vector<size_t> &blob_sizes,
                          std::vector<TieredSchema> &output,
                          const api::Context &api_context) {
  (void)api_context;
  Status result = 0;

  // TODO(chogan): Return a TieredSchema that minimizes a cost function F given
  // a set of N Tiers and a blob, while satisfying a policy P.

  // TODO(chogan): This should be part of the Context
  PlacementPolicy policy = PlacementPolicy::kRandom;

  switch (policy) {
    case PlacementPolicy::kRandom: {
      result = RandomPlacement(context, rpc, blob_sizes, output);
      break;
    }
    case PlacementPolicy::kTopDown: {
      result = TopDownPlacement(context, rpc, blob_sizes, output);
      break;
    }
  }

  return result;
}

}  // namespace hermes
