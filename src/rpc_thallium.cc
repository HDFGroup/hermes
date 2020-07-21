#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <string>

#include "rpc.h"

#include <abt.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace tl = thallium;

namespace hermes {

struct ThalliumState {
  char server_name_prefix[16];
  char server_name_postfix[8];
  tl::engine *engine;
};

/**
 *  Lets Thallium know how to serialize a BufferID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param buffer_id The BufferID to serialize.
 */
template<typename A>
void serialize(A &ar, BufferID &buffer_id) {
  ar & buffer_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a BucketID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param bucket_id The BucketID to serialize.
 */
template<typename A>
void serialize(A &ar, BucketID &bucket_id) {
  ar & bucket_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a BlobID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param blob_id The BlobID to serialize.
 */
template<typename A>
void serialize(A &ar, BlobID &blob_id) {
  ar & blob_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a MapType.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param map_type The MapType to serialize.
 */
template<typename A>
void serialize(A &ar, MapType &map_type) {
  ar & (int)map_type;
}

std::string GetHostNumberAsString(RpcContext *rpc, u32 node_id) {
  std::string result = "";
  if (rpc->host_number_range[0] != 0 && rpc->host_number_range[1] != 0) {
    result = std::to_string(node_id + rpc->host_number_range[0] - 1);
  }

  return result;
}

template<typename ReturnType, typename... Ts>
auto RpcCall(RpcContext *rpc, u32 node_id, const char *func_name, Ts... args) {
  ThalliumState *tl_state = (ThalliumState *)rpc->state;

  std::string host_number = GetHostNumberAsString(rpc, node_id);
  std::string host_name = std::string(rpc->base_hostname) + host_number;

  const int max_ip_address_size = 16;
  char ip_address[max_ip_address_size];
  // TODO(chogan): @errorhandling
  // TODO(chogan): @optimization Could cache the last N hostname->IP mappings to
  // avoid excessive syscalls. Should profile first.
  struct hostent *hostname_info = gethostbyname(host_name.c_str());
  in_addr **addr_list = (struct in_addr **)hostname_info->h_addr_list;
  // TODO(chogan): @errorhandling
  strncpy(ip_address, inet_ntoa(*addr_list[0]), max_ip_address_size);

  std::string server_name = (std::string(tl_state->server_name_prefix) +
                             std::string(ip_address) +
                             std::string(tl_state->server_name_postfix));

  // TODO(chogan): Support all protocols
  const char *protocol = "tcp";
  // TODO(chogan): Save connections instead of creating them for every rpc
  tl::engine engine(protocol, THALLIUM_CLIENT_MODE);
  tl::remote_procedure remote_proc = engine.define(func_name);
  tl::endpoint server = engine.lookup(server_name);

  if constexpr(std::is_same<ReturnType, void>::value)
  {
    remote_proc.disable_response();
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

void ThalliumStartRpcServer(SharedMemoryContext *context, RpcContext *rpc,
                            const char *addr, i32 num_rpc_threads) {
  ThalliumState *state = (ThalliumState *)rpc->state;
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

  function<void(const request&, BufferID, std::vector<u8>, size_t, size_t)>
    rpc_write_buffer_by_id = [context](const request &req, BufferID id,
                                       std::vector<u8> data,
                                       size_t bytes_left_to_write,
                                       size_t offset) {

      Blob blob = {};
      blob.size = data.size();
      blob.data = data.data();
      size_t result = LocalWriteBufferById(context, id, blob,
                                           bytes_left_to_write, offset);

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

  function<void(const request&)> rpc_finalize =
    [&rpc_server](const request &req) {
      (void)req;
      rpc_server->finalize();
    };

  rpc_server->define("GetBuffers", rpc_get_buffers);
  rpc_server->define("RemoteReleaseBuffer",
                    rpc_release_buffer).disable_response();
  rpc_server->define("SplitBuffers", rpc_split_buffers).disable_response();
  rpc_server->define("MergeBuffers", rpc_merge_buffers).disable_response();
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

  rpc_server->define("Finalize", rpc_finalize).disable_response();

  // TODO(chogan): Currently the calling thread waits for finalize because
  // that's the way the tests are set up, but once the RPC server is started
  // from Hermes initialization the calling thread will need to continue
  // executing.
  // rpc_server->wait_for_finalize();
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

void FinalizeRpcContext(RpcContext *rpc) {
  ThalliumState *state = (ThalliumState *)rpc->state;
  delete state->engine;
}

}  // namespace hermes
