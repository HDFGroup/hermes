#ifndef HERMES_DATA_PLACEMENT_ENGINE_H_
#define HERMES_DATA_PLACEMENT_ENGINE_H_

#include "hermes_types.h"
#include "hermes.h"

namespace hermes {

class DataPlacementEngine {
};

TieredSchema CalculatePlacement(size_t blob_size, const api::Context &ctx);

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
