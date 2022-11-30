//
// Created by lukemartinlogan on 11/30/22.
//

#ifndef HERMES_SRC_RPC_FACTORY_H_
#define HERMES_SRC_RPC_FACTORY_H_

#include "communication.h"
#include "communication_mpi.cc"
#include <memory>

namespace hermes {
class CommunicationFactory {
 public:
  /**
   * Return an RPC. Uses factory pattern.
   *
   * @param type, RpcType, type of mapper to be used by the STDIO adapter.
   * @return Instance of RPC given a type.
   */
  static std::unique_ptr<CommunicationContext> Get(
      const CommunicationType &type) {
    switch (type) {
      case CommunicationType::kMpi: {
        return std::make_unique<MpiCommunicator>();
      }
      default:
        return nullptr;
    }
  }
};
}  // namespace hermes

#endif  // HERMES_SRC_RPC_FACTORY_H_
