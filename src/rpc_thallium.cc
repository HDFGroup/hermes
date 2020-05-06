#include "rpc.h"

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace tl = thallium;

namespace hermes {

struct ThalliumState {
  char *server_name;
};

template<typename ReturnType, typename... Ts>
auto RpcCall(RpcContext *rpc, u32 node_id, const char *func_name, Ts... args) {
  // TODO(chogan): Store this in the metadata arena
  ThalliumState *tl_state = (ThalliumState *)rpc->state;
  (void)tl_state;
  (void)node_id;
  const char kServerName[] = "ofi+sockets://localhost:8080";
  // TODO(chogan): Server must include the node_id
  tl::engine engine("tcp", THALLIUM_CLIENT_MODE);
  tl::remote_procedure remote_proc = engine.define(func_name);
  tl::endpoint server = engine.lookup(kServerName);

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

// TODO(chogan): addr should be in the RpcContext
void ThalliumStartRpcServer(SharedMemoryContext *context, RpcContext *rpc,
                            const char *addr, i32 num_rpc_threads) {
  tl::engine rpc_server(addr, THALLIUM_SERVER_MODE, false, num_rpc_threads);

  LOG(INFO) << "Serving at " << rpc_server.self() << " with "
            << num_rpc_threads << " RPC threads" << std::endl;

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

  function<void(const request&)> rpc_finalize =
    [&rpc_server](const request &req) {
      (void)req;
      rpc_server.finalize();
    };

  rpc_server.define("GetBuffers", rpc_get_buffers);
  rpc_server.define("RemoteReleaseBuffer", rpc_release_buffer).disable_response();
  rpc_server.define("SplitBuffers", rpc_split_buffers).disable_response();
  rpc_server.define("MergeBuffers", rpc_merge_buffers).disable_response();
  rpc_server.define("RemoteGet", rpc_map_get);
  rpc_server.define("RemotePut", rpc_map_put).disable_response();
  rpc_server.define("RemoteDelete", rpc_map_delete).disable_response();
  rpc_server.define("RemoteAddBlobIdToBucket", rpc_add_blob).disable_response();
  rpc_server.define("RemoteDestroyBucket",
                    rpc_destroy_bucket).disable_response();
  rpc_server.define("RemoteGetBufferIdList", rpc_get_buffer_id_list);
  rpc_server.define("RemoteFreeBufferIdList",
                    rpc_free_buffer_id_list).disable_response();
  rpc_server.define("Finalize", rpc_finalize).disable_response();

  // TODO(chogan): Currently the calling thread waits for finalize because
  // that's the way the tests are set up, but once the RPC server is started
  // from Hermes initialization the calling thread will need to continue
  // executing.
  rpc_server.wait_for_finalize();
}

void InitRpcContext(RpcContext *rpc, u32 num_nodes, u32 node_id) {
  rpc->num_nodes = num_nodes;
  rpc->node_id = node_id;
  rpc->start_server = ThalliumStartRpcServer;
}

}  // namespace hermes
