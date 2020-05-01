#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace hermes {

struct ThalliumState {
  char *server_name;
};

u64 ThalliumCall1(const char *func_name, std::string name, MapType map_type) {
  // TODO(chogan): Store this in the metadata arena
  // TODO(chogan): Server must include the node_id
  const char kServerName[] = "ofi+sockets://localhost:8080";
  tl::engine engine("tcp", THALLIUM_CLIENT_MODE);
  tl::remote_procedure remote_get = engine.define(func_name);
  tl::endpoint server = engine.lookup(kServerName);
  u64 result = remote_get.on(server)(name, map_type);

  return result;
}

void ThalliumCall2(const char *func_name, const std::string &name, u64 val,
                   MapType map_type) {
  // TODO(chogan): Store this in the metadata arena
  // TODO(chogan): Server must include the node_id
  const char kServerName[] = "ofi+sockets://localhost:8080";
  tl::engine engine("tcp", THALLIUM_CLIENT_MODE);
  tl::remote_procedure remote_put = engine.define(func_name);
  tl::endpoint server = engine.lookup(kServerName);
  remote_put.on(server)(name, val, map_type);
}

void ThalliumStartRpcServer(SharedMemoryContext *context, const char *addr,
                            i32 num_rpc_threads) {
  tl::engine rpc_server(addr, THALLIUM_SERVER_MODE, false, num_rpc_threads);

  LOG(INFO) << "Serving at " << rpc_server.self() << " with "
            << num_rpc_threads << " RPC threads" << std::endl;

  using std::function;
  using std::vector;
  using tl::request;

  // BufferPool requests

  function<void(const request&, const TieredSchema&)> rpc_get_buffers =
    [context](const request &req, const TieredSchema &schema) {
      std::vector<BufferID> result = GetBuffers(context, schema);
      req.respond(result);
    };

  function<void(const request&, const vector<BufferID>&)> rpc_release_buffers =
    [context](const request &req, const vector<BufferID> &buffer_ids) {
      (void)req;
      ReleaseBuffers(context, buffer_ids);
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

  function<void(const request&, std::string, MapType)> rpc_map_get =
    [context](const request &req, std::string name, MapType map_type) {
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      u64 result = LocalGet(mdm, name.c_str(), map_type);

      req.respond(result);
    };

  function<void(const request&, const std::string&, u64, MapType)> rpc_map_put =
    [context](const request &req, const std::string &name, u64 val,
              MapType map_type) {
      (void)req;
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      LocalPut(mdm, name.c_str(), val, map_type);
    };

  function<void(const request&, BucketID, BlobID)> rpc_add_blob =
    [context](const request &req, BucketID bucket_id, BlobID blob_id) {
      (void)req;
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      LocalAddBlobIdToBucket(mdm, bucket_id, blob_id);
    };

  function<void(const request&)> rpc_finalize =
    [&rpc_server](const request &req) {
      (void)req;
      rpc_server.finalize();
    };

  rpc_server.define("GetBuffers", rpc_get_buffers);
  rpc_server.define("ReleaseBuffers", rpc_release_buffers).disable_response();
  rpc_server.define("SplitBuffers", rpc_split_buffers).disable_response();
  rpc_server.define("MergeBuffers", rpc_merge_buffers).disable_response();
  rpc_server.define("RemoteGet", rpc_map_get);
  rpc_server.define("RemotePut", rpc_map_put).disable_response();
  rpc_server.define("RemoteAddBlobIdToBucket", rpc_add_blob).disable_response();
  rpc_server.define("Finalize", rpc_finalize).disable_response();

  // TODO(chogan): Currently the calling thread waits for finalize because
  // that's the way the tests are set up, but once the RPC server is started
  // from Hermes initialization the calling thread will need to continue
  // executing.
  rpc_server.wait_for_finalize();
}

void InitRpcContext(RpcContext *rpc) {
  rpc->call1 = ThalliumCall1;
  rpc->call2 = ThalliumCall2;
  rpc->start_server = ThalliumStartRpcServer;
}

}  // namespace hermes
