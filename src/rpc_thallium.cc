#include <string>

#include "rpc.h"

namespace tl = thallium;

namespace hermes {

std::string GetHostNumberAsString(RpcContext *rpc, u32 node_id) {
  std::string result = "";
  if (rpc->host_number_range[0] != 0 && rpc->host_number_range[1] != 0) {
    result = std::to_string(node_id + rpc->host_number_range[0] - 1);
  }

  return result;
}

void CopyStringToCharArray(const std::string &src, char *dest) {
  size_t src_size = src.size();
  memcpy(dest, src.c_str(), src_size);
  dest[src_size] = '\0';
}

void ThalliumStartRpcServer(SharedMemoryContext *context, RpcContext *rpc,
                            const char *addr, i32 num_rpc_threads) {
  ThalliumState *state = GetThalliumState(rpc);
  state->engine = new tl::engine(addr, THALLIUM_SERVER_MODE, true,
                                 num_rpc_threads);

  tl::engine *rpc_server = state->engine;

  std::string rpc_server_name = rpc_server->self();
  LOG(INFO) << "Serving at " << rpc_server_name << " with "
            << num_rpc_threads << " RPC threads" << std::endl;

  size_t end_of_protocol = rpc_server_name.find_first_of(":");
  std::string server_name_prefix = rpc_server_name.substr(0, end_of_protocol) +
                                   "://";
  CopyStringToCharArray(server_name_prefix, state->server_name_prefix);

  std::string server_name_postfix = ":" + std::to_string(rpc->port);
  CopyStringToCharArray(server_name_postfix, state->server_name_postfix);

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

  function<void(const request&, BufferID)> rpc_get_buffer_size =
    [context](const request &req, BufferID id) {
      u32 result = LocalGetBufferSize(context, id);

      req.respond(result);
    };

  function<void(const request&, BufferID, std::vector<u8>, size_t)>
    rpc_write_buffer_by_id = [context](const request &req, BufferID id,
                                       std::vector<u8> data, size_t offset) {

      Blob blob = {};
      blob.size = data.size();
      blob.data = data.data();
      size_t result = LocalWriteBufferById(context, id, blob, offset);

      req.respond(result);

    };

  function<void(const request&, BufferID, size_t)> rpc_read_buffer_by_id =
    [context](const request &req, BufferID id, size_t size) {
      std::vector<u8> result(size);
      Blob blob = {};
      blob.size = size;
      blob.data = result.data();
      [[maybe_unused]] size_t bytes_read = LocalReadBufferById(context, id,
                                                               &blob, 0);
      assert(bytes_read == size);

      req.respond(result);
    };

  // Metadata requests

  function<void(const request&, string, const MapType&)> rpc_map_get =
    [context](const request &req, string name, const MapType &map_type) {
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      u64 result = LocalGet(mdm, name.c_str(), map_type);

      req.respond(result);
    };

  function<void(const request&, const string&, u64, const MapType&)> rpc_map_put =
    [context](const request &req, const string &name, u64 val,
              const MapType &map_type) {
      (void)req;
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      LocalPut(mdm, name.c_str(), val, map_type);
    };

  function<void(const request&, string, const MapType&)> rpc_map_delete =
    [context](const request &req, string name, const MapType &map_type) {
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

  function<void(const request&, const string&, BlobID)>
    rpc_destroy_blob_by_name =
    [context, rpc](const request &req, const string &name, BlobID id) {
      (void)req;
      LocalDestroyBlobByName(context, rpc, name.c_str(), id);
    };

  function<void(const request&, BlobID)>
    rpc_destroy_blob_by_id = [context, rpc](const request &req, BlobID id) {
      (void)req;
      LocalDestroyBlobById(context, rpc, id);
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

  function<void(const request&)> rpc_get_global_tier_capacities =
    [context](const request &req) {
      std::vector<u64> result = LocalGetGlobalTierCapacities(context);

      req.respond(result);
    };

  // TODO(chogan): Only need this on mdm->global_system_view_state_node_id.
  // Probably should move it to a completely separate tl::engine.
  function<void(const request&, std::vector<i64>)> rpc_update_global_system_view_state =
    [context](const request &req, std::vector<i64> adjustments) {
      (void)req;
      LocalUpdateGlobalSystemViewState(context, adjustments);
    };

  function<void(const request&)> rpc_finalize =
    [rpc](const request &req) {
      (void)req;
      ThalliumState *state = GetThalliumState(rpc);
      state->engine->finalize();
    };

  // TODO(chogan): Currently these three are only used for testing.
  rpc_server->define("GetBuffers", rpc_get_buffers);
  rpc_server->define("SplitBuffers", rpc_split_buffers).disable_response();
  rpc_server->define("MergeBuffers", rpc_merge_buffers).disable_response();
  //

  rpc_server->define("RemoteReleaseBuffer",
                     rpc_release_buffer).disable_response();
  rpc_server->define("RemoteGetBufferSize", rpc_get_buffer_size);

  rpc_server->define("RemoteReadBufferById", rpc_read_buffer_by_id);
  rpc_server->define("RemoteWriteBufferById", rpc_write_buffer_by_id);

  rpc_server->define("RemoteGet", rpc_map_get);
  rpc_server->define("RemotePut", rpc_map_put).disable_response();
  rpc_server->define("RemoteDelete", rpc_map_delete).disable_response();
  rpc_server->define("RemoteAddBlobIdToBucket",
                     rpc_add_blob).disable_response();
  rpc_server->define("RemoteDestroyBucket",
                    rpc_destroy_bucket).disable_response();
  rpc_server->define("RemoteRenameBucket",
                     rpc_rename_bucket).disable_response();
  rpc_server->define("RemoteDestroyBlobByName",
                     rpc_destroy_blob_by_name).disable_response();
  rpc_server->define("RemoteDestroyBlobById",
                     rpc_destroy_blob_by_id).disable_response();
  rpc_server->define("RemoteContainsBlob", rpc_contains_blob);
  rpc_server->define("RemoteGetNextFreeBucketId", rpc_get_next_free_bucket_id);
  rpc_server->define("RemoteRemoveBlobFromBucketInfo",
                    rpc_remove_blob_from_bucket_info).disable_response();
  rpc_server->define("RemoteAllocateBufferIdList", rpc_allocate_buffer_id_list);
  rpc_server->define("RemoteGetBufferIdList", rpc_get_buffer_id_list);
  rpc_server->define("RemoteFreeBufferIdList",
                    rpc_free_buffer_id_list).disable_response();
  rpc_server->define("RemoteIncrementRefcount",
                    rpc_increment_refcount).disable_response();
  rpc_server->define("RemoteDecrementRefcount",
                    rpc_decrement_refcount).disable_response();
  rpc_server->define("RemoteUpdateGlobalSystemViewState",
                     rpc_update_global_system_view_state).disable_response();
  rpc_server->define("RemoteGetGlobalTierCapacities",
                     rpc_get_global_tier_capacities);
  rpc_server->define("RemoteFinalize", rpc_finalize).disable_response();
}


void StartGlobalSystemViewStateUpdateThread(SharedMemoryContext *context,
                                            RpcContext *rpc, Arena *arena,
                                            double sleep_ms) {

  struct ThreadArgs {
    SharedMemoryContext *context;
    RpcContext *rpc;
    double sleep_ms;
  };

  auto update_global_system_view_state = [](void *args) {
    ThreadArgs *targs = (ThreadArgs *)args;
    ThalliumState *state = GetThalliumState(targs->rpc);
    while (!state->kill_requested.load()) {
      UpdateGlobalSystemViewState(targs->context, targs->rpc);
      tl::thread::self().sleep(*state->engine, targs->sleep_ms);
    }
  };

  ThreadArgs *args = PushStruct<ThreadArgs>(arena);
  args->context = context;
  args->rpc = rpc;
  args->sleep_ms = sleep_ms;

  ThalliumState *state = GetThalliumState(rpc);
  ABT_xstream_create(ABT_SCHED_NULL, &state->execution_stream);
  ABT_thread_create_on_xstream(state->execution_stream,
                               update_global_system_view_state, args,
                               ABT_THREAD_ATTR_NULL, NULL);
}

void InitRpcContext(RpcContext *rpc, u32 num_nodes, u32 node_id,
                     Config *config) {
  rpc->num_nodes = num_nodes;
  rpc->node_id = node_id;
  rpc->start_server = ThalliumStartRpcServer;
  rpc->state_size = sizeof(ThalliumState);
  rpc->port = config->rpc_port;
  CopyStringToCharArray(config->rpc_server_base_name, rpc->base_hostname);
  rpc->host_number_range[0] = config->rpc_host_number_range[0];
  rpc->host_number_range[1] = config->rpc_host_number_range[1];
}

void *CreateRpcState(Arena *arena) {
  ThalliumState *result = PushClearedStruct<ThalliumState>(arena);

  return result;
}

void FinalizeRpcContext(RpcContext *rpc, bool is_daemon) {
  ThalliumState *state = GetThalliumState(rpc);
  state->kill_requested.store(true);
  ABT_xstream_join(state->execution_stream);
  ABT_xstream_free(&state->execution_stream);

  if (is_daemon) {
    state->engine->wait_for_finalize();
  } else {
    state->engine->finalize();
  }

  delete state->engine;
}

}  // namespace hermes
