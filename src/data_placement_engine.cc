#include "data_placement_engine.h"

#include <assert.h>
#include <math.h>

#include <utility>

#include "hermes.h"

namespace hermes {

const SystemViewState *GetSystemViewState(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  const SystemViewState *result =
    (SystemViewState *)((u8 *)mdm + mdm->system_view_state_offset);

  return result;
}

enum class PlacementPolicy {
  kRandom,
  kTopDown,
};

// TODO(chogan): Unfinished sketch
TieredSchema TopDownPlacement(SharedMemoryContext *context, size_t blob_size) {
  HERMES_NOT_IMPLEMENTED_YET;

  TieredSchema result;
  const SystemViewState *state = GetSystemViewState(context);
  size_t size_left = blob_size;
  TierID current_tier = 0;

  while (size_left > 0 && current_tier < state->num_tiers) {
    size_t bytes_used = 0;
    if (state->bytes_available[current_tier] > size_left) {
      bytes_used = size_left;
    } else {
      bytes_used = state->bytes_available[current_tier];
      current_tier++;
    }

    if (bytes_used) {
      size_left -= bytes_used;
      result.push_back(std::make_pair(current_tier, bytes_used));
    }
  }

  if (size_left > 0) {
    // TODO(chogan): Trigger BufferOrganizer
    // EvictBuffers(eviction_schema);
    result.clear();
  }

  return result;
}

TieredSchema RandomPlacement(SharedMemoryContext *context, size_t blob_size) {
  TieredSchema result;
  const SystemViewState *state = GetSystemViewState(context);
  TierID tier_id = rand() % state->num_tiers;

  if (state->bytes_available[tier_id] > blob_size) {
    result.push_back(std::make_pair(blob_size, tier_id));
  } else {
    HERMES_NOT_IMPLEMENTED_YET;
    // TODO(chogan): Trigger BufferOrganizer
    // EvictBuffers(eviction_schema);
  }

  return result;
}

TieredSchema CalculatePlacement(SharedMemoryContext *context, size_t blob_size,
                                const api::Context &api_context) {
  (void)api_context;

  // TODO(chogan): Return a TieredSchema that minimizes a cost function F given
  // a set of N Tiers and a blob, while satisfying a policy P.

  // TODO(chogan): This should be part of the Context
  PlacementPolicy policy = PlacementPolicy::kRandom;

  TieredSchema result;
  switch (policy) {
    case PlacementPolicy::kRandom: {
      result = RandomPlacement(context, blob_size);
      break;
    }
    case PlacementPolicy::kTopDown: {
      result = TopDownPlacement(context, blob_size);
      break;
    }
  }

  return result;
}

}  // namespace hermes
