#ifndef HERMES_RPC_THALLIUM_H_
#define HERMES_RPC_THALLIUM_H_

#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace hermes {

const int kMaxServerNamePrefix = 32;
const int kMaxServerNamePostfix = 8;

struct ThalliumState {
  char server_name_prefix[kMaxServerNamePrefix];
  char server_name_postfix[kMaxServerNamePostfix];
  char bo_server_name_postfix[kMaxServerNamePostfix];
  std::atomic<bool> kill_requested;
  tl::engine *engine;
  tl::engine *bo_engine;
  tl::engine *client_engine;
  ABT_xstream execution_stream;
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
 *  Lets Thallium know how to serialize a TargetID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The TargetID to serialize.
 */
template<typename A>
void serialize(A &ar, TargetID &target_id) {
  ar & target_id.as_int;
}

template<typename A>
void serialize(A &ar, SwapBlob &swap_blob) {
  ar & swap_blob.node_id;
  ar & swap_blob.offset;
  ar & swap_blob.size;
  ar & swap_blob.bucket_id;
}

#ifndef THALLIUM_USE_CEREAL
/**
 *  Lets Thallium know how to serialize a MapType.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param map_type The MapType to serialize.
 */
template<typename A>
void save(A &ar, MapType &map_type) {
  int val = (int)map_type;
  ar.write(&val, 1);
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
void load(A &ar, MapType &map_type) {
  int val = 0;
  ar.read(&val, 1);
  map_type = (MapType)val;
}
#endif

std::string GetRpcAddress(Config *config, const std::string &host_number,
                          int port);

static inline ThalliumState *GetThalliumState(RpcContext *rpc) {
  ThalliumState *result = (ThalliumState *)rpc->state;

  return result;
}

template<typename ReturnType, typename... Ts>
ReturnType RpcCall(RpcContext *rpc, u32 node_id, const char *func_name,
                   Ts... args) {
  ThalliumState *state = GetThalliumState(rpc);
  std::string server_name = GetServerName(rpc, node_id);
  tl::remote_procedure remote_proc = state->client_engine->define(func_name);
  // TODO(chogan): @optimization We can save a little work by storing the
  // endpoint instead of looking it up on every call
  tl::endpoint server = state->client_engine->lookup(server_name);

  if constexpr(std::is_same<ReturnType, void>::value) {
    remote_proc.disable_response();
    remote_proc.on(server)(std::forward<Ts>(args)...);
  } else {
    ReturnType result = remote_proc.on(server)(std::forward<Ts>(args)...);

    return result;
  }
}

}  // namespace hermes

#endif  // HERMES_RPC_THALLIUM_H_
