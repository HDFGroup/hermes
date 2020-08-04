#ifndef HERMES_RPC_THALLIUM_H_
#define HERMES_RPC_THALLIUM_H_

#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace tl = thallium;

namespace hermes {

struct ThalliumState {
  char server_name_prefix[16];
  char server_name_postfix[8];
  std::atomic<bool> kill_requested;
  tl::engine *engine;
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

static inline ThalliumState *GetThalliumState(RpcContext *rpc) {
  ThalliumState *result = (ThalliumState *)rpc->state;

  return result;
}

template<typename ReturnType, typename... Ts>
ReturnType RpcCall(RpcContext *rpc, u32 node_id, const char *func_name, Ts... args) {
  ThalliumState *tl_state = GetThalliumState(rpc);

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

  std::string prefix = std::string(tl_state->server_name_prefix);
  // NOTE(chogan): Chop "://" off the end of the server_name_prefix to get the
  // protocol
  std::string protocol = prefix.substr(0, prefix.length() - 3);

  // TODO(chogan): Save connections instead of creating them for every rpc
  tl::engine engine(protocol, THALLIUM_CLIENT_MODE, true);
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

}  // namespace hermes

#endif  // HERMES_RPC_THALLIUM_H_
