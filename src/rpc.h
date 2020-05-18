#ifndef HERMES_RPC_H_
#define HERMES_RPC_H_

#include <string>
#include <vector>

#include "hermes_types.h"
#include "metadata_management.h"

namespace hermes {

struct RpcContext;

typedef void (*StartFunc)(SharedMemoryContext*, RpcContext*, const char*, int);

struct RpcContext {
  void *state;
  u32 node_id;
  u32 num_nodes;

  StartFunc start_server;
};

void InitRpcContext(RpcContext *rpc, u32 num_nodes, u32 node_id);

}  // namespace hermes

#endif  // HERMES_RPC_H_
