#include "data_placement_engine.h"
#include "buffer_pool.h"

namespace hermes {

struct SystemViewState {
  u64 bytes_available[kMaxTiers];
  int num_tiers;
};

SystemViewState GetSystemViewState() {
  SystemViewState result = {};
  // TODO(chogan): The BufferPool should feed an Apollo hook this info
  result.num_tiers = 4;
  u64 fifty_mb = 1024 * 1024 * 50;
  result.bytes_available[0] = fifty_mb;
  result.bytes_available[1] = fifty_mb;
  result.bytes_available[2] = fifty_mb;
  result.bytes_available[3] = fifty_mb;

  return result;
}

enum class PlacementPolicy {
  kRandom,
  kTopDown,
};

// TODO(chogan): Unfinished sketch
TierSchema TopDownPlacement(size_t blob_size) {
  TieredSchema result;
  SystemViewState state = GetSystemViewState();
  size_t size_left = blob_size;
  TierID current_tier = 0;

  while (size_left > 0 && current_tier < state.num_tiers) {
    size_t bytes_used = 0;
    if (state.bytes_available[current_tier] > size_left) {
      bytes_used = size_left;
    } else {
      bytes_used = state.bytes_available[current_tier];
      current_tier++;
    }

    if (bytes_used) {
      size_left -= bytes_used;
      result.push_back(std::make_pair(current_tier, bytes_used));
    }
  }

  if (size_left > 0) {
    // Couldn't fit everything, trigger BufferOrganizer
    // EvictBuffers(eviction_schema);
    result.clear();
  }

  return result;
}

TieredSchema RandomPlacement(size_t blob_size) {
  TieredSchema result;
  SystemViewState state = GetSystemViewState();
  TierID tier_id = rand() % state.num_tiers;

  if (state.bytes_available[tier_id] > blob_size) {
    result.push_back(std::make_pair(tier_id, blob_size));
  } else {
    assert(!"Overflowing buffers not yet supported in RandomPlacement\n");
  }

  return result;
}

TieredSchema CalculatePlacement(size_t blob_size, const api::Context &ctx) {
  (void)ctx;

  // TODO(chogan): Return a TieredSchema that minimizes a cost function F given
  // a set of N Tiers and a blob, while satisfying a policy P.

  // TODO(chogan): This should be part of the Context
  PlacementPolicy policy = PlacementPolicy::kRandom;

  TieredSchema result;
  switch (policy) {
    case kRandom: {
      result = RandomPlacement(blob_size);
    }
    case kTopDown: {
      result = TopDownPlacement(blob_size);
    }
  }

  return result;
}

#if 0
Status Put(const std::string &name, const Blob &data, Context &ctx) {
  Status ret = 0;
  SharedMemoryContext *context = 0;
  TieredSchema schema = CalculatePlacement(data.size(), ctx);
  while (schema.size() == 0) {
    schema = CalculatePlacement(data.siz(), ctx);
  }
  std::vector<BufferID> bufferIDs = GetBuffers(context, schema);
  while (bufferIDs.size() == 0) {
    bufferIDs = GetBuffers(context, schema);
  }
  WriteBlobToBuffers(context, data, bufferIDs);
  // UpdateMDM();
  return ret;
}
#endif

}  // namespace hermes
