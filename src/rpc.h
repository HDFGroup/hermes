/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_RPC_H_
#define HERMES_RPC_H_

#include <string>
#include <vector>

#include "hermes_types.h"
#include "communication.h"
#include "rpc_decorator.h"
#include "config.h"

#include <labstor/data_structures/lockless/string.h>
#include <labstor/data_structures/lockless/vector.h>

namespace hermes {

/** RPC types */
enum class RpcType {
  kThallium
};

/**
   A structure to represent RPC context.
 */
class RpcContext {
 public:
  COMM_TYPE &comm_;
  /** The hermes configuration used to initialize this RPC */
  ServerConfig &config_;
  /** Array of host names stored in shared memory. This array size is
   * RpcContext::num_nodes. */
  lipcl::vector<lipcl::string> host_names_;
  int port_;  /**< port number */

 public:
  explicit RpcContext(COMM_TYPE &comm, ServerConfig &config, u32 num_nodes,
                      u32 node_id) : comm_(comm), config_(config) {
    port_ = config.rpc_.port_;
    switch (comm.type_) {
      case HermesType::kColocated
      case HermesType::kServer: {
        // Load configuration from either file or config
        break;
      }
      case HermesType::kClient: {
        // Load hostnames from SHMEM
        break;
      }
    }
  }

  /** Check if we should skip an RPC and call a function locally */
  bool ShouldDoLocalCall(int node_id) {
    switch (comm_.type_) {
      case HermesType::kClient: {
        return false;
      }
      case HermesType::kServer: {
        return node_id == comm_->node_id_;
      }
      case HermesType::kColocated: {
        return true;
      }
    }
  }

  /** get RPC address */
  std::string GetRpcAddress(u32 node_id, int port) {
    std::string result = config_.rpc_.protocol_ + "://";

    if (!config_.rpc_.domain_.empty()) {
      result += config_.rpc_.domain_ + "/";
    }
    std::string host_name = GetHostNameFromNodeId(node_id);
    result += host_name + ":" + std::to_string(port);

    return result;
  }

  /** get host name from node ID */
  std::string GetHostNameFromNodeId(u32 node_id) {
    std::string result;
    // NOTE(chogan): node_id 0 is reserved as the NULL node
    u32 index = node_id - 1;
    result = host_names_[index].str();
    return result;
  }
};

}  // namespace hermes

#if defined(HERMES_RPC_THALLIUM)
#include "rpc_thallium.h"
#define RPC_TYPE ThalliumRpc
#else
#error "RPC implementation required (e.g., -DHERMES_RPC_THALLIUM)."
#endif

#endif  // HERMES_RPC_H_
