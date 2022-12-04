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

namespace hermes {

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
  /** the hermes configuration used to initialize this RPC */
  ServerConfig *config_;
  int port_;  /**< port number */
  int node_id_; /**< the ID of this node*/
  std::vector<HostInfo> hosts_; /**< Hostname and ip addr per-node */

 public:
  explicit RpcContext(COMM_TYPE *comm, ServerConfig *config)
      : comm_(comm), config_(config) {}

  /** initialize host info list */
  void InitHostInfo() {
    port_ = config_->rpc_.port_;
    if (hosts_.size()) { return; }
    auto &hosts = config_->rpc_.host_names_;
    // Load hosts from hostfile
    if (!config_->rpc_.host_file_.empty()) {
      // TODO(llogan): load host names from hostfile
    }

    // Get all host info
    hosts_.resize(hosts.size());
    for (const auto& name : hosts) {
      hosts_.emplace_back(name, _GetIpAddress(name));
    }

    // Get id of current host
    int node_id = 1;  // NOTE(llogan): node_ids start from 1 (0 is NULL)
    std::string my_ip = _GetMyIpAddress();
    for (const auto& host_info : hosts_) {
      if (host_info.ip_addr_ == my_ip) {
        node_id_ = node_id;
        break;
      }
      ++node_id;
    }
  }

  /** Check if we should skip an RPC and call a function locally */
  bool ShouldDoLocalCall(int node_id) {
    switch (comm_->type_) {
      case HermesType::kClient: {
        return false;
      }
      case HermesType::kServer: {
        return node_id == node_id_;
      }
      case HermesType::kColocated: {
        return true;
      }
    }
  }

  /** get RPC address */
  std::string GetRpcAddress(u32 node_id, int port) {
    std::string result = config_->rpc_.protocol_ + "://";
    if (!config_->rpc_.domain_.empty()) {
      result += config_->rpc_.domain_ + "/";
    }
    std::string host_name = GetHostNameFromNodeId(node_id);
    result += host_name + ":" + std::to_string(port);
    return result;
  }

  /** Get RPC address for this node */
  std::string GetMyRpcAddress() {
    return GetRpcAddress(node_id_, port_);
  }

  /** get host name from node ID */
  std::string GetHostNameFromNodeId(u32 node_id) {
    // NOTE(chogan): node_id 0 is reserved as the NULL node
    u32 index = node_id - 1;
    return hosts_[index].hostname_;
  }

  /** get host name from node ID */
  std::string GetIpAddressFromNodeId(u32 node_id) {
    // NOTE(chogan): node_id 0 is reserved as the NULL node
    u32 index = node_id - 1;
    return hosts_[index].ip_addr_;
  }


 private:
  /** Get the IPv4 address of this machine */
  std::string _GetMyIpAddress() {
    return _GetIpAddress("localhost");
  }

  /** Get IPv4 address from the host with "host_name" */
  std::string _GetIpAddress(const std::string &host_name) {
    struct hostent hostname_info = {};
    struct hostent *hostname_result;
    int hostname_error = 0;
    char hostname_buffer[4096] = {};
#ifdef __APPLE__
    hostname_result = gethostbyname(host_name.c_str());
    in_addr **addr_list = (struct in_addr **)hostname_result->h_addr_list;
#else
    int gethostbyname_result =
        gethostbyname_r(host_name.c_str(), &hostname_info, hostname_buffer,
                        4096, &hostname_result, &hostname_error);
    if (gethostbyname_result != 0) {
      LOG(FATAL) << hstrerror(h_errno);
    }
    in_addr **addr_list = (struct in_addr **)hostname_info.h_addr_list;
#endif
    if (!addr_list[0]) {
      LOG(FATAL) << hstrerror(h_errno);
    }

    char ip_address[INET_ADDRSTRLEN];
    const char *inet_result =
        inet_ntop(AF_INET, addr_list[0], ip_address, INET_ADDRSTRLEN);
    if (!inet_result) {
      FailedLibraryCall("inet_ntop");
    }
    return ip_address;
  }

 public:
  virtual void InitServer() = 0;
  virtual void InitClient() = 0;
  virtual void InitColocated() = 0;
};

}  // namespace hermes

#include "rpc_factory.h"

#endif  // HERMES_RPC_H_
