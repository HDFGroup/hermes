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

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <string>
#include <vector>

#include "hermes_types.h"
#include "communication.h"
#include "rpc_decorator.h"
#include "config.h"
#include "utils.h"

#include <labstor/data_structures/lockless/string.h>
#include <labstor/data_structures/lockless/vector.h>

namespace hermes::api {
class Hermes;
}  // namespace hermes::api

namespace hermes {

struct MetadataManager;
using api::Hermes;

/** RPC types */
enum class RpcType {
  kThallium
};

struct HostInfo {
  int node_id_;
  std::string hostname_;
  std::string ip_addr_;

  HostInfo() = default;
  explicit HostInfo(const std::string &hostname, const std::string &ip_addr)
      : hostname_(hostname), ip_addr_(ip_addr) {}
};

/**
   A structure to represent RPC context.
 */
class RpcContext {
 public:
  COMM_TYPE *comm_;
  ServerConfig *config_;
  MetadataManager *mdm_;
  int port_;  /**< port number */
  int node_id_; /**< the ID of this node*/
  std::vector<HostInfo> hosts_; /**< Hostname and ip addr per-node */

 public:
  RpcContext() = default;

  /** initialize host info list */
  void InitRpcContext();

  /** Check if we should skip an RPC and call a function locally */
  bool ShouldDoLocalCall(int node_id);

  /** get RPC address */
  std::string GetRpcAddress(u32 node_id, int port);

  /** Get RPC address for this node */
  std::string GetMyRpcAddress();

  /** get host name from node ID */
  std::string GetHostNameFromNodeId(u32 node_id);

  /** get host name from node ID */
  std::string GetIpAddressFromNodeId(u32 node_id);

  /** Get RPC protocol */
  std::string GetProtocol();

 private:
  /** Get the IPv4 address of this machine */
  std::string _GetMyIpAddress();

  /** Get IPv4 address from the host with "host_name" */
  std::string _GetIpAddress(const std::string &host_name);

 public:
  virtual void InitServer() = 0;
  virtual void InitClient() = 0;
  virtual void InitColocated() = 0;
};

}  // namespace hermes

#include "rpc_factory.h"

#endif  // HERMES_RPC_H_
