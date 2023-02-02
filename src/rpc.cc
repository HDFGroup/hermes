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

#include "rpc.h"
#include "hermes.h"

namespace hermes {

/** initialize host info list */
void RpcContext::InitRpcContext() {
  comm_ = &HERMES->comm_;
  config_ = &HERMES->server_config_;
  port_ = config_->rpc_.port_;
  if (hosts_.size()) { return; }
  auto &hosts = config_->rpc_.host_names_;
  // Load hosts from hostfile
  if (!config_->rpc_.host_file_.empty()) {
    // TODO(llogan): load host names from hostfile
  }

  // Get all host info
  hosts_.reserve(hosts.size());
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
bool RpcContext::ShouldDoLocalCall(int node_id) {
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
    default: {
      LOG(FATAL) << "Invalid HermesType" << std::endl;
    }
  }
}

/** get RPC address */
std::string RpcContext::GetRpcAddress(u32 node_id, int port) {
  std::string result = config_->rpc_.protocol_ + "://";
  if (!config_->rpc_.domain_.empty()) {
    result += config_->rpc_.domain_ + "/";
  }
  std::string host_name = GetHostNameFromNodeId(node_id);
  result += host_name + ":" + std::to_string(port);
  return result;
}

/** Get RPC address for this node */
std::string RpcContext::GetMyRpcAddress() {
  return GetRpcAddress(node_id_, port_);
}

/** get host name from node ID */
std::string RpcContext::GetHostNameFromNodeId(u32 node_id) {
  // NOTE(chogan): node_id 0 is reserved as the NULL node
  u32 index = node_id - 1;
  return hosts_[index].hostname_;
}

/** get host name from node ID */
std::string RpcContext::GetIpAddressFromNodeId(u32 node_id) {
  // NOTE(chogan): node_id 0 is reserved as the NULL node
  u32 index = node_id - 1;
  return hosts_[index].ip_addr_;
}

/** Get RPC protocol */
std::string RpcContext::GetProtocol() {
  return config_->rpc_.protocol_;
}

/** Get the IPv4 address of this machine */
std::string RpcContext::_GetMyIpAddress() {
  return _GetIpAddress("localhost");
}

/** Get IPv4 address from the host with "host_name" */
std::string RpcContext::_GetIpAddress(const std::string &host_name) {
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

}
