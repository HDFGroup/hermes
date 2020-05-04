#ifndef HERMES_RPC_H_
#define HERMES_RPC_H_

#include <string>

#include "hermes_types.h"
#include "metadata_management.h"

namespace hermes {

// TODO(chogan): Templatize one `call` function
typedef u64 (*Call1Func)(const char *, std::string, MapType map_type);
typedef void (*Call2Func)(const char *, const std::string&, u64,
                          MapType map_type);
typedef void (*Call3Func)(const char *, BucketID, BlobID);
typedef void (*StartFunc)(SharedMemoryContext *, const char *, i32);

struct RpcContext {
  void *state;

  Call1Func call1;
  Call2Func call2;
  Call3Func call3;
  StartFunc start_server;
};

void InitRpcContext(RpcContext *rpc);

}  // namespace hermes

#endif  // HERMES_RPC_H_
