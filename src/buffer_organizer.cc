#include "hermes.h"
#include "data_placement_engine.h"

namespace hapi = hermes::api;

namespace hermes {

int PlaceInHierarchy(SharedMemoryContext *context, RpcContext *rpc,
                      SwapBlob swap_blob) {
  int result = 0;
  // Read data from PFS into blob
  std::vector<u8> blob_mem(swap_blob.size);
  Blob blob = {};
  blob.data = blob_mem.data();
  blob.size = blob_mem.size();
  size_t bytes_read = ReadFromSwap(context, blob, swap_blob);

  // std::vector<PlacementSchema> schemas;
  // std::vector<size_t> sizes;
  // api::Context ctx;
  // api::Status ret = CalculatePlacement(context, rpc, sizes, schemas, ctx);
  // if (ret == 0) {
  //   std::vector<std::string> names(1, name);
  //   std::vector<std::vector<u8>> blobs(1);
  //   blobs[0].resize(size);
  //   // TODO(chogan): Create a PreallocatedMemory allocator for std::vector so
  //   // that a single-blob-Put doesn't perform a copy
  //   for (size_t i = 0; i < size; ++i) {
  //     blobs[0][i] = data[i];
  //   }
  //   ret = PlaceBlobs(schemas, blobs, names);
  // } else {
    // TODO(chogan): @errorhandling
  // }

  return result;
}

int MoveToTarget(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id,
                 TargetID dest) {
// TODO(chogan): Move blob from current location to Target dest
  HERMES_NOT_IMPLEMENTED_YET;
  int result = 0;
  return result;
}

}  // namespace hermes
