#ifndef HERMES_RPC_H_
#define HERMES_RPC_H_

#include <string>
#include <vector>

#include "hermes_types.h"
#include "metadata_management.h"

namespace hermes {

struct RpcContext;

const int kMaxServerNameSize = 128;

typedef void (*StartFunc)(SharedMemoryContext*, RpcContext*, const char*, int);

struct RpcContext {
  void *state;
  /** The size of the internal rpc state. */
  size_t state_size;
  u32 node_id;
  u32 num_nodes;
  int port;
  /** Convenient way of specifiying multiple hostnames if the pattern is
   * predictable (i.e., if running on three nodes with names cluster-node-6,
   * cluster-node-7, and cluster-node-8, the first_host_number would be 6 and
   * Hermes can generate the other two). */
  u32 first_host_number;
  /** The host name without the host number. Allows programmatic
   * construction of predictable host names like cluster-node-1,
   * cluster-node-2, etc. without storing extra copies of the base hostname.*/
  char base_hostname[kMaxServerNameSize];

  // TODO(chogan): Also allow reading hostnames from a file for heterogeneous or
  // non-contiguous hostnames (e.g., compute-node-20, compute-node-30,
  // storage-node-16, storage-node-24)
  // char *host_file_name;

  StartFunc start_server;
};

void *InitRpcContext(RpcContext *rpc, Arena *arena, u32 num_nodes, u32 node_id,
                     Config *config);

}  // namespace hermes

#endif  // HERMES_RPC_H_
