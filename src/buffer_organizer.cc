#include "hermes.h"
#include "data_placement_engine.h"

namespace hermes {

Status PlaceBlob(SharedMemoryContext *context, RpcContext *rpc,
                 PlacementSchema &schema, Blob blob, char *name,
                 BucketID bucket_id) {
  Status result = 0;
  std::vector<BufferID> buffer_ids = GetBuffers(context, schema);
  if (buffer_ids.size()) {
    MetadataManager *mdm = GetMetadataManagerFromContext(context);
    char *bucket_name = ReverseGetFromStorage(mdm, bucket_id.as_int,
                                              kMapType_Bucket);
    LOG(INFO) << "Attaching blob " << name << " to Bucket "
              << bucket_name << std::endl;
    WriteBlobToBuffers(context, rpc, blob, buffer_ids);

    // NOTE(chogan): Update all metadata associated with this Put
    AttachBlobToBucket(context, rpc, name, bucket_id, buffer_ids);
  } else {
    // TODO(chogan): @errorhandling
    result = 1;
  }

  return result;
}

int PlaceInHierarchy(SharedMemoryContext *context, RpcContext *rpc,
                      SwapBlob swap_blob) {
  int result = 0;
  std::vector<u8> blob_mem(swap_blob.size);
  Blob blob = {};
  blob.data = blob_mem.data();
  blob.size = blob_mem.size();
  size_t bytes_read = ReadFromSwap(context, blob, swap_blob);

  std::vector<PlacementSchema> schemas;
  std::vector<size_t> sizes(1, blob.size);
  api::Context ctx;
  api::Status ret = CalculatePlacement(context, rpc, sizes, schemas, ctx);
  if (ret == 0) {
    MetadataManager *mdm = GetMetadataManagerFromContext(context);
    char *name = ReverseGetFromStorage(mdm, swap_blob.blob_id.as_int,
                                       kMapType_Blob);
    ret = PlaceBlob(context, rpc, schemas[0], blob, name, swap_blob.bucket_id);
  } else {
    // TODO(chogan): @errorhandling
    result = 1;
  }

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
