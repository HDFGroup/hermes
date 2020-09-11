#ifndef HERMES_DATA_PLACEMENT_ENGINE_H_
#define HERMES_DATA_PLACEMENT_ENGINE_H_

#include "hermes_types.h"
#include "hermes.h"

namespace hermes {

using api::Status;

class DataPlacementEngine {
  static inline size_t count_device_ {};

public:
  size_t getCountDevice() const {return count_device_;}
  void setCountDevice(size_t new_count_device) {
    count_device_ = new_count_device;
  }
};

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          std::vector<size_t> &blob_size,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context);

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
