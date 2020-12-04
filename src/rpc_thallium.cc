#include <string>

#include "rpc.h"
#include "metadata_management_internal.h"

namespace tl = thallium;

namespace hermes {

std::string GetHostNumberAsString(RpcContext *rpc, u32 node_id) {
  std::string result = "";
  if (rpc->host_number_range[0] != 0 && rpc->host_number_range[1] != 0) {
    result = std::to_string(node_id + rpc->host_number_range[0] - 1);
  }

  return result;
}

void CopyStringToCharArray(const std::string &src, char *dest, size_t max) {
  size_t src_size = src.size();
  if (src_size >= max) {
    LOG(WARNING) << "Can only fit " << max << " characters from the string "
                 << src << std::endl;
  }
  size_t copy_size = std::min(max - 1, src_size);
  memcpy(dest, src.c_str(), copy_size);
  dest[copy_size] = '\0';
}

void ThalliumStartRpcServer(SharedMemoryContext *context, RpcContext *rpc,
                            Arena *arena, const char *addr,
                            i32 num_rpc_threads) {
  ThalliumState *state = GetThalliumState(rpc);
  state->engine = new tl::engine(addr, THALLIUM_SERVER_MODE, true,
                                 num_rpc_threads);

  tl::engine *rpc_server = state->engine;

  std::string rpc_server_name = rpc_server->self();
  LOG(INFO) << "Serving at " << rpc_server_name << " with "
            << num_rpc_threads << " RPC threads" << std::endl;

  size_t end_of_protocol = rpc_server_name.find_first_of(":");
  std::string server_name_prefix =
    rpc_server_name.substr(0, end_of_protocol) + "://";
  CopyStringToCharArray(server_name_prefix, state->server_name_prefix,
                        kMaxServerNamePrefix);

  std::string server_name_postfix = ":" + std::to_string(rpc->port);
  CopyStringToCharArray(server_name_postfix, state->server_name_postfix,
                        kMaxServerNamePostfix);

  using std::function;
  using std::string;
  using std::vector;
  using tl::request;

  // BufferPool requests

  function<void(const request&, const PlacementSchema&)> rpc_get_buffers =
    [context](const request &req, const PlacementSchema &schema) {
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

  function<void(const request&, BufferID)> rpc_read_buffer_by_id =
    [context](const request &req, BufferID id) {
      BufferHeader *header = GetHeaderByBufferId(context, id);
      std::vector<u8> result(header->used);
      Blob blob = {};
      blob.size = result.size();
      blob.data = result.data();
      [[maybe_unused]] size_t bytes_read = LocalReadBufferById(context, id,
                                                               &blob, 0);
      assert(bytes_read == result.size());

      req.respond(result);
    };

  function<void(const request&, tl::bulk&, BufferID)>
    rpc_bulk_read_buffer_by_id =
    [context, rpc_server, arena](const request &req, tl::bulk &bulk,
                                 BufferID id) {
      tl::endpoint endpoint = req.get_endpoint();
      BufferHeader *header = GetHeaderByBufferId(context, id);
      ScopedTemporaryMemory temp_memory(arena);

      u8 *buffer_data = 0;
      size_t size = header->used;

      if (BufferIsByteAddressable(context, id)) {
        buffer_data = GetRamBufferPtr(context, id);
      } else {
        // TODO(chogan): Probably need a way to lock the trans_arena. Currently
        // an assertion will fire if multiple threads try to use it at once.
        if (size > GetRemainingCapacity(temp_memory)) {
          // TODO(chogan): Need to transfer in a loop if we don't have enough
          // temporary memory available
          HERMES_NOT_IMPLEMENTED_YET;
        }
        buffer_data = PushSize(temp_memory, size);
        Blob blob = {};
        blob.data = buffer_data;
        blob.size = size;
        size_t read_offset = 0;
        LocalReadBufferById(context, id, &blob, read_offset);
      }

      std::vector<std::pair<void*, size_t>> segments(1);
      segments[0].first  = buffer_data;
      segments[0].second = size;
      tl::bulk local_bulk = rpc_server->expose(segments,
                                               tl::bulk_mode::read_only);

      size_t bytes_read = local_bulk >> bulk.on(endpoint);
      // TODO(chogan): @errorhandling
      assert(bytes_read == size);

      req.respond(bytes_read);
    };

  // Metadata requests

  function<void(const request&, string, const MapType&)> rpc_map_get =
    [context](const request &req, string name, const MapType &map_type) {
      MetadataManager *mdm = GetMetadataManagerFromContext(context);
      u64 result = LocalGet(mdm, name.c_str(), map_type);

      req.respond(result);
    };

  function<void(const request&, const string&, u64, const MapType&)>
    rpc_map_put = [context](const request &req, const string &name, u64 val,
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

  function<void(const request&)> rpc_get_global_device_capacities =
    [context](const request &req) {
      std::vector<u64> result = LocalGetGlobalDeviceCapacities(context);

      req.respond(result);
    };

  function<void(const request&, TargetID id)> rpc_get_remaining_capacity =
    [context](const request &req, TargetID id) {
      u64 result = LocalGetRemainingCapacity(context, id);

      req.respond(result);
    };

  // TODO(chogan): Only need this on mdm->global_system_view_state_node_id.
  // Probably should move it to a completely separate tl::engine.
  function<void(const request&, std::vector<i64>)>
    rpc_update_global_system_view_state =
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
  rpc_server->define("RemoteBulkReadBufferById", rpc_bulk_read_buffer_by_id);

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
  rpc_server->define("RemoteGetRemainingCapacity", rpc_get_remaining_capacity);
  rpc_server->define("RemoteUpdateGlobalSystemViewState",
                     rpc_update_global_system_view_state).disable_response();
  rpc_server->define("RemoteGetGlobalDeviceCapacities",
                     rpc_get_global_device_capacities);
  rpc_server->define("RemoteFinalize", rpc_finalize).disable_response();
}

void StartBufferOrganizer(SharedMemoryContext *context, RpcContext *rpc,
                          const char *addr, int num_threads, int port) {
  ThalliumState *state = GetThalliumState(rpc);

  state->bo_engine = new tl::engine(addr, THALLIUM_SERVER_MODE, true,
                                    num_threads);
  tl::engine *rpc_server = state->bo_engine;

  std::string rpc_server_name = rpc_server->self();
  LOG(INFO) << "Buffer organizer serving at " << rpc_server_name << " with "
            << num_threads << " RPC threads" << std::endl;

  std::string server_name_postfix = ":" + std::to_string(port);
  CopyStringToCharArray(server_name_postfix, state->bo_server_name_postfix,
                        kMaxServerNamePostfix);

  auto rpc_place_in_hierarchy = [context, rpc](const tl::request &req,
                                               SwapBlob swap_blob,
                                               const std::string name,
                                               int retries) {
    (void)req;
    for (int i = 0; i < retries; ++i) {
      LOG(INFO) << "Buffer Organizer placing blob '" << name
                << "' in hierarchy. Attempt " << i + 1 << " of " << retries
                << std::endl;
      int result = PlaceInHierarchy(context, rpc, swap_blob, name);
      if (result == 0) {
        break;
      } else {
        // TODO(chogan): We probably don't want to sleep here, but for now this
        // enables testing.
        double sleep_ms = 2000;
        ThalliumState *state = GetThalliumState(rpc);

        if (state && state->bo_engine) {
          tl::thread::self().sleep(*state->bo_engine, sleep_ms);
        }
      }
    }
  };

  auto rpc_move_to_target = [context, rpc](const tl::request &req,
                                           SwapBlob swap_blob,
                                           TargetID target_id, int retries) {
    (void)req;
    (void)swap_blob;
    (void)target_id;
    for (int i = 0; i < retries; ++i) {
      // TODO(chogan): MoveToTarget(context, rpc, target_id, swap_blob);
      HERMES_NOT_IMPLEMENTED_YET;
      int result = 0;
      if (result == 0) {
        break;
      }
    }
  };

  rpc_server->define("PlaceInHierarchy",
                     rpc_place_in_hierarchy).disable_response();
  rpc_server->define("MoveToTarget",
                     rpc_move_to_target).disable_response();
}

void TriggerBufferOrganizer(RpcContext *rpc, const char *func_name,
                            const std::string &blob_name, SwapBlob swap_blob,
                            int retries) {
  std::string server_name = GetServerName(rpc, rpc->node_id, true);
  std::string protocol = GetProtocol(rpc);
  tl::engine engine(protocol, THALLIUM_CLIENT_MODE, true);
  tl::remote_procedure remote_proc = engine.define(func_name);
  tl::endpoint server = engine.lookup(server_name);
  remote_proc.disable_response();
  // TODO(chogan): Templatize?
  remote_proc.on(server)(swap_blob, blob_name, retries);
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
  CopyStringToCharArray(config->rpc_server_base_name, rpc->base_hostname,
                        kMaxServerNameSize);
  CopyStringToCharArray(config->rpc_server_suffix, rpc->hostname_suffix,
                        kMaxServerSuffixSize);
  rpc->host_number_range[0] = config->rpc_host_number_range[0];
  rpc->host_number_range[1] = config->rpc_host_number_range[1];

  rpc->client_rpc.state_size = sizeof(ClientThalliumState);
}

void *CreateRpcState(Arena *arena) {
  ThalliumState *result = PushClearedStruct<ThalliumState>(arena);

  return result;
}

std::string GetProtocol(RpcContext *rpc) {
  ThalliumState *tl_state = GetThalliumState(rpc);

  std::string prefix = std::string(tl_state->server_name_prefix);
  // NOTE(chogan): Chop "://" off the end of the server_name_prefix to get the
  // protocol
  std::string result = prefix.substr(0, prefix.length() - 3);

  return result;
}

void InitRpcClients(RpcContext *rpc) {
  // TODO(chogan): Need a per-client persistent arena
  ClientThalliumState *state =
    (ClientThalliumState *)malloc(sizeof(ClientThalliumState));
  std::string protocol = GetProtocol(rpc);
  // TODO(chogan): This should go in a per-client persistent arena
  state->engine = new tl::engine(protocol, THALLIUM_CLIENT_MODE, true);

  rpc->client_rpc.state = state;
}

void ShutdownRpcClients(RpcContext *rpc) {
  ClientThalliumState *state = GetClientThalliumState(rpc);
  if (state) {
    if (state->engine) {
      delete state->engine;
    }
    free(state);
  }
}

void FinalizeRpcContext(RpcContext *rpc, bool is_daemon) {
  ThalliumState *state = GetThalliumState(rpc);
  state->kill_requested.store(true);
  ABT_xstream_join(state->execution_stream);
  ABT_xstream_free(&state->execution_stream);

  if (is_daemon) {
    state->engine->wait_for_finalize();
    state->bo_engine->wait_for_finalize();
  } else {
    state->engine->finalize();
    state->bo_engine->finalize();
  }

  delete state->engine;
  delete state->bo_engine;
}

std::string GetRpcAddress(Config *config, const std::string &host_number,
                          int port) {
  std::string result = config->rpc_protocol + "://";

  if (!config->rpc_domain.empty()) {
    result += config->rpc_domain + "/";
  }
  result += (config->rpc_server_base_name + host_number +
             config->rpc_server_suffix + ":" + std::to_string(port));

  return result;
}

std::string GetServerName(RpcContext *rpc, u32 node_id,
                          bool is_buffer_organizer) {
  ThalliumState *tl_state = GetThalliumState(rpc);

  std::string host_number = GetHostNumberAsString(rpc, node_id);
  std::string host_name = (std::string(rpc->base_hostname) + host_number +
                           std::string(rpc->hostname_suffix));
  const int kMaxIpAddressSize = 16;
  char ip_address[kMaxIpAddressSize];
  // TODO(chogan): @errorhandling
  // TODO(chogan): @optimization Could cache the last N hostname->IP mappings to
  // avoid excessive syscalls. Should profile first.
  struct hostent *hostname_info = gethostbyname(host_name.c_str());
  in_addr **addr_list = (struct in_addr **)hostname_info->h_addr_list;
  // TODO(chogan): @errorhandling
  strncpy(ip_address, inet_ntoa(*addr_list[0]), kMaxIpAddressSize);

  std::string result = std::string(tl_state->server_name_prefix);

  result += std::string(ip_address);

  if (is_buffer_organizer) {
    result += std::string(tl_state->bo_server_name_postfix);
  } else {
    result += std::string(tl_state->server_name_postfix);
  }

  return result;
}

size_t BulkRead(RpcContext *rpc, u32 node_id, const char *func_name,
                u8 *data, size_t max_size, BufferID id) {
  std::string server_name = GetServerName(rpc, node_id);
  std::string protocol = GetProtocol(rpc);

  tl::engine engine(protocol, THALLIUM_CLIENT_MODE, true);
  tl::remote_procedure remote_proc = engine.define(func_name);
  tl::endpoint server = engine.lookup(server_name);

  std::vector<std::pair<void*, size_t>> segments(1);
  segments[0].first  = data;
  segments[0].second = max_size;

  tl::bulk bulk = engine.expose(segments, tl::bulk_mode::write_only);
  size_t result = remote_proc.on(server)(bulk, id);

  return result;
}

}  // namespace hermes
