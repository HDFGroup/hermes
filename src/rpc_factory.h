//
// Created by lukemartinlogan on 11/30/22.
//

#ifndef HERMES_SRC_RPC_FACTORY_H_
#define HERMES_SRC_RPC_FACTORY_H_

#include "rpc.h"
#include "rpc_thallium.h"

namespace hermes {
class RpcFactory {
 public:
  /**
   * Return an RPC. Uses factory pattern.
   *
   * @param type, RpcType, type of mapper to be used by the STDIO adapter.
   * @return Instance of RPC given a type.
   */
  static std::unique_ptr<RpcContext> Get(
      const RpcType &type,
      CommunicationContext *comm,
      SharedMemoryContext *context,
      u32 num_nodes, u32 node_id, Config *config) {
    switch (type) {
      case RpcType::kThallium: {
        return std::make_unique<ThalliumRpc>(
            comm, context, num_nodes, node_id, config);
      }
      default:
        return nullptr;
    }
  }
};
}  // namespace hermes

#endif  // HERMES_SRC_RPC_FACTORY_H_
