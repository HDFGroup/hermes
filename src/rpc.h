#ifndef HERMES_RPC_H_
#define HERMES_RPC_H_

#include <string>

#include "hermes_types.h"

namespace hermes {

typedef u64 (*Call1Func)(const char *, std::string);
typedef void (*Call2Func)(const char *, const std::string&, u64);
typedef void (*StartFunc)(SharedMemoryContext *, const char *, i32);

struct RpcContext {
  void *state;

  Call1Func call1;
  Call2Func call2;
  StartFunc start_server;
};

void InitRpcContext(RpcContext *rpc);

}  // namespace hermes

#endif  // HERMES_RPC_H_
