#include "data_placement_engine.h"

namespace hermes {

struct SystemViewState {
  u64 capacities[kMaxTiers];
};

SystemViewState GetSystemViewState() {
  SystemViewState result = {};

  return result;
}

TieredSchema CalculatePlacement(size_t blob_size, const api::Context &ctx) {
  SystemViewState state = GetSystemViewState();

  // TODO(chogan): Return a TieredSchema that minimizes a cost function F given
  // a set of N Tiers and a blob, while satisfying a policy P.

  // NOTE(chogan): Currently just placing into the highest available Tier.
  TieredSchema result;

  BufferPool *pool = GetBufferPoolFromContext(context);
  int num_tiers = pool->num_tiers;
  size_t num_bytes = blob_size;

  for (TierID tier_id = 0; tier_id < num_tiers; ++tier_id) {

    result.push_back(std::make_pair(tier_id, num_bytes));
  }

  // TODO(chogan): Could trigger BufferOrganizer

  return result;
}

#if 0
Status Put(const std::string &name, const Blob &data, Context &ctx) {
  Status ret = 0;
  SharedMemoryContext *context = 0;
  TieredSchema schema = CalculatePlacement(data.size(), ctx);
  std::vector<BufferID> bufferIDs = GetBuffers(context, schema);
  WriteBlobToBuffers(context, data, bufferIDs);
  // UpdateMDM();
  return ret;
}
#endif

}  // namespace hermes
