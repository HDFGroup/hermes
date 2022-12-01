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
#include "metadata_management.h"
#include <labstor/data_structures/lockless/string.h>
#include <labstor/data_structures/lockless/vector.h>

namespace hermes {

const int kMaxServerNameSize = 128;  /**< maximum size of server name */
const int kMaxServerSuffixSize = 16; /**< maximum size of server suffix */

/** RPC types */
enum class RpcType {
  kThallium
};

/**
   A structure to represent RPC context.
 */
namespace lipcl = labstor::ipc::lockless;

class RpcContext {
 public:
  CommunicationContext *comm_;
  SharedMemoryContext *context_;
  Config *config_;  /** The hermes configuration used to initialize this RPC */
  /** Array of host names stored in shared memory. This array size is
   * RpcContext::num_nodes. */
  lipcl::vector<lipcl::string> host_names;
  u32 node_id_;        /**< node ID */
  u32 num_nodes_;      /**< number of nodes */
  // The number of host numbers in the rpc_host_number_range entry of the
  // configuration file. Not necessarily the number of nodes because when there
  // is only 1 node, the entry can be left blank, or contain 1 host number.
  int port_;           /**< port number */
  bool use_host_file_; /**< use host file if true */

  // TODO(chogan): Also allow reading hostnames from a file for heterogeneous or
  // non-contiguous hostnames (e.g., compute-node-20, compute-node-30,
  // storage-node-16, storage-node-24)
  // char *host_file_name;

 public:
  explicit RpcContext(CommunicationContext *comm,
                      SharedMemoryContext *context,
                      u32 num_nodes, u32 node_id, Config *config) :
      comm_(comm), context_(context), num_nodes_(num_nodes),
      node_id_(node_id), config_(config) {
    port_ = config->rpc_port;
    if (!config->rpc_server_host_file.empty()) {
      use_host_file_ = true;
    }
  }

  /** get RPC address */
  std::string GetRpcAddress(u32 node_id, int port) {
    std::string result = config_->rpc_protocol + "://";

    if (!config_->rpc_domain.empty()) {
      result += config_->rpc_domain + "/";
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
    result = host_names[index].str();
    return result;
  }
};

}  // namespace hermes

// TODO(chogan): I don't like that code similar to this is in buffer_pool.cc.
// I'd like to only have it in one place.
#if defined(HERMES_RPC_THALLIUM)
#include "rpc_thallium.h"
#else
#error "RPC implementation required (e.g., -DHERMES_RPC_THALLIUM)."
#endif

#endif  // HERMES_RPC_H_
