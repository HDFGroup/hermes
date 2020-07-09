#include "rpc.h"

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace tl = thallium;

namespace hermes {

struct ThalliumState {
  char server_name_prefix[kMaxServerNameSize];
  char server_name_postfix[8];
};

template<typename ReturnType, typename... Ts>
auto RpcCall(RpcContext *rpc, u32 node_id, const char *func_name, Ts... args) {
  // TODO(chogan): Store this in the metadata arena
  ThalliumState *tl_state = (ThalliumState *)rpc->state;
  std::string host_number = std::to_string(rpc->first_host_number +
                                           node_id - 1);
  if (host_number == "0") {
    // TODO(chogan): Allow host numbers of 0. Need to check the host name range
    // from the config.
    host_number = "";
  }
  std::string server_name = (std::string(tl_state->server_name_prefix) +
                             host_number +
                             std::string(tl_state->server_name_postfix));

  // TODO(chogan): Support all protocols
  const char *protocol = "tcp";
  // TODO(chogan): Save connections instead of creating them for every rpc
  tl::engine engine(protocol, THALLIUM_CLIENT_MODE);
  tl::remote_procedure remote_proc = engine.define(func_name);
  tl::endpoint server = engine.lookup(server_name);

  if constexpr(std::is_same<ReturnType, void>::value)
  {
    remote_proc.on(server)(std::forward<Ts>(args)...);
  }
  else
  {
    ReturnType result = remote_proc.on(server)(std::forward<Ts>(args)...);

    return result;
  }
}

void CopyStringToCharArray(const std::string &src, char *dest) {
  size_t src_size = src.size();
  memcpy(dest, src.c_str(), src_size);
  dest[src_size] = '\0';
}

// TODO(chogan): addr should be in the RpcContext
void ThalliumStartRpcServer(SharedMemoryContext *context, RpcContext *rpc,
                            const char *addr, i32 num_rpc_threads) {
  tl::engine rpc_server(addr, THALLIUM_SERVER_MODE, false, num_rpc_threads);

  std::string rpc_server_name = rpc_server.self();
  LOG(INFO) << "Serving at " << rpc_server_name << " with "
            << num_rpc_threads << " RPC threads" << std::endl;

  ThalliumState *tl_state = (ThalliumState *)rpc->state;
  size_t end_of_protocol = rpc_server_name.find_first_of(":");
  std::string server_name_prefix = rpc_server_name.substr(0, end_of_protocol) +
                                   "://" + std::string(rpc->base_hostname);
  CopyStringToCharArray(server_name_prefix, tl_state->server_name_prefix);

  std::string server_name_postfix = ":" + std::to_string(rpc->port);
  CopyStringToCharArray(server_name_postfix, tl_state->server_name_postfix);

  using std::function;
  using std::string;
  using std::vector;
  using tl::request;

  // BufferPool requests

  function<void(const request&, const TieredSchema&)> rpc_get_buffers =
    [context](const request &req, const TieredSchema &schema) {
      std::vector<BufferID> result = GetBuffers(context, schema);
      req.respond(result);
    };

  function<void(const request&, BufferID)> rpc_release_buffer =
    [context](const request &req, BufferID id) {
      (void)req;
      LocalReleaseBuffer(context, id);
    };

  function<void(const request&, int)> rpc_split_buffers =
    [context](const request &req, int slab_index) {
      (void)req;
      SplitRamBufferFreeList(context, slab_index);
    };

  function<void(const request&, int)> rpc_merge_buffers =
    [context](const request &req, int slab_index) {
      (void)req;
      MergeRamBufferFreeList(context, slab_index);
    };

  // Metadata requests

  function<void(const request&, string, MapType)> rpc_map_get =
    [context](const request &req, string name, MapType map_type) {
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      u64 result = LocalGet(mdm, name.c_str(), map_type);

      req.respond(result);
    };

  function<void(const request&, const string&, u64, MapType)> rpc_map_put =
    [context](const request &req, const string &name, u64 val,
              MapType map_type) {
      (void)req;
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      LocalPut(mdm, name.c_str(), val, map_type);
    };

  function<void(const request&, string, MapType)> rpc_map_delete =
    [context](const request &req, string name, MapType map_type) {
      (void)req;
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      LocalDelete(mdm, name.c_str(), map_type);
    };

  function<void(const request&, BucketID, BlobID)> rpc_add_blob =
    [context](const request &req, BucketID bucket_id, BlobID blob_id) {
      (void)req;
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      LocalAddBlobIdToBucket(mdm, bucket_id, blob_id);
    };

  function<void(const request&, BlobID)> rpc_get_buffer_id_list =
    [context](const request &req, BlobID blob_id) {
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      std::vector<BufferID> result = LocalGetBufferIdList(mdm, blob_id);

      req.respond(result);
    };

  function<void(const request&, BlobID)> rpc_free_buffer_id_list =
    [context](const request &req, BlobID blob_id) {
      (void)req;
      LocalFreeBufferIdList(context, blob_id);
    };

  function<void(const request&, const string&, BucketID)> rpc_destroy_bucket =
    [context, rpc](const request &req, const string &name, BucketID id) {
      (void)req;
      LocalDestroyBucket(context, rpc, name.c_str(), id);
    };

  function<void(const request&, const string&)>
    rpc_get_next_free_bucket_id = [context, rpc](const request &req,
                                                 const string &name) {
      BucketID result = LocalGetNextFreeBucketId(context, rpc, name);

      req.respond(result);
    };

  function<void(const request&, const vector<BufferID>&)>
    rpc_allocate_buffer_id_list =
      [context](const request &req, const vector<BufferID> &buffer_ids) {
        MetadataManager *mdm = GetMetadataManagerFromContext(context);
        u32 result = LocalAllocateBufferIdList(mdm, buffer_ids);

        req.respond(result);
    };

  function<void(const request&, BucketID, const string&, const string&)>
    rpc_rename_bucket = [context, rpc](const request &req, BucketID id,
                                       const string &old_name,
                                       const string &new_name) {
      (void)req;
      LocalRenameBucket(context, rpc, id, old_name.c_str(), new_name.c_str());
    };

  function<void(const request&, const string&, BlobID)> rpc_destroy_blob =
    [context, rpc](const request &req, const string &name, BlobID id) {
      (void)req;
      LocalDestroyBlob(context, rpc, name.c_str(), id);
    };
  function<void(const request&, BucketID, BlobID)> rpc_contains_blob =
    [context](const request &req, BucketID bucket_id, BlobID blob_id) {
      bool result = LocalContainsBlob(context, bucket_id, blob_id);
      req.respond(result);
    };

  function<void(const request&, BucketID, BlobID)>
    rpc_remove_blob_from_bucket_info = [context](const request &req,
                                                 BucketID bucket_id,
                                                 BlobID blob_id) {
      (void)req;
      LocalRemoveBlobFromBucketInfo(context, bucket_id, blob_id);
    };

  function<void(const request&, BucketID)> rpc_increment_refcount =
    [context](const request &req, BucketID id) {
      (void)req;
      LocalIncrementRefcount(context, id);
    };

  function<void(const request&, BucketID)> rpc_decrement_refcount =
    [context](const request &req, BucketID id) {
      (void)req;
      LocalDecrementRefcount(context, id);
    };

  function<void(const request&)> rpc_finalize =
    [&rpc_server](const request &req) {
      (void)req;
      rpc_server.finalize();
    };

  rpc_server.define("GetBuffers", rpc_get_buffers);
  rpc_server.define("RemoteReleaseBuffer",
                    rpc_release_buffer).disable_response();
  rpc_server.define("SplitBuffers", rpc_split_buffers).disable_response();
  rpc_server.define("MergeBuffers", rpc_merge_buffers).disable_response();
  rpc_server.define("RemoteGet", rpc_map_get);
  rpc_server.define("RemotePut", rpc_map_put).disable_response();
  rpc_server.define("RemoteDelete", rpc_map_delete).disable_response();
  rpc_server.define("RemoteAddBlobIdToBucket", rpc_add_blob).disable_response();
  rpc_server.define("RemoteDestroyBucket",
                    rpc_destroy_bucket).disable_response();
  rpc_server.define("RemoteRenameBucket", rpc_rename_bucket).disable_response();
  rpc_server.define("RemoteDestroyBlob", rpc_destroy_blob).disable_response();
  rpc_server.define("RemoteContainsBlob", rpc_contains_blob);
  rpc_server.define("RemoteRemoveBlobFromBucketInfo",
                    rpc_remove_blob_from_bucket_info).disable_response();
  rpc_server.define("RemoteGetBufferIdList", rpc_get_buffer_id_list);
  rpc_server.define("RemoteFreeBufferIdList",
                    rpc_free_buffer_id_list).disable_response();
  rpc_server.define("RemoteIncrementRefcount",
                    rpc_increment_refcount).disable_response();
  rpc_server.define("RemoteDecrementRefcount",
                    rpc_decrement_refcount).disable_response();
  rpc_server.define("Finalize", rpc_finalize).disable_response();

  // TODO(chogan): Currently the calling thread waits for finalize because
  // that's the way the tests are set up, but once the RPC server is started
  // from Hermes initialization the calling thread will need to continue
  // executing.
  // rpc_server.wait_for_finalize();
}

void *InitRpcContext(RpcContext *rpc, Arena *arena, u32 num_nodes, u32 node_id,
                     Config *config) {
  rpc->num_nodes = num_nodes;
  rpc->node_id = node_id;
  rpc->start_server = ThalliumStartRpcServer;
  rpc->state_size = sizeof(ThalliumState);
  rpc->port = config->rpc_port;
  CopyStringToCharArray(config->rpc_server_base_name, rpc->base_hostname);
  rpc->first_host_number = config->rpc_host_number_range[0];

  ThalliumState *result = PushClearedStruct<ThalliumState>(arena);

  return result;
}

}  // namespace hermes
