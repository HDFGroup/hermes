#ifndef HERMES_DATA_PLACEMENT_ENGINE_H_
#define HERMES_DATA_PLACEMENT_ENGINE_H_

#include "hermes_types.h"
#include "hermes.h"

namespace hermes {

using api::Status;

class DataPlacementEngine {
};

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          std::vector<size_t> &blob_size,
                          std::vector<TieredSchema> &output,
                          const api::Context &api_context);

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
