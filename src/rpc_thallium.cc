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

#include <string>

#include "rpc_thallium.h"
#include "singleton.h"

namespace tl = thallium;

namespace hermes {

/** start Thallium RPC server */
void ThalliumRpc::InitServer() {
  std::string addr = GetMyRpcAddress();
  server_engine_ = tl::engine(addr,
                              THALLIUM_SERVER_MODE,
                              true,
                              config_->rpc_.num_threads_);
  std::string rpc_server_name = server_engine_.self();
  LOG(INFO) << "Serving at " << rpc_server_name << " with "
            << config_->rpc_.num_threads_ << " RPC threads" << std::endl;
  DefineRpcs();
}

/** initialize RPC clients */
void ThalliumRpc::InitClient() {
  std::string protocol = GetProtocol();
  client_engine_ = tl::engine(protocol,
                              THALLIUM_CLIENT_MODE,
                              true, 1);
}

/** finalize RPC context */
void ThalliumRpc::Finalize() {
}

/** run daemon */
void ThalliumRpc::RunDaemon() {
}

/** get server name */
std::string ThalliumRpc::GetServerName(u32 node_id) {
  std::string ip_address = GetIpAddressFromNodeId(node_id);
  return config_->rpc_.protocol_ + "://" +
         std::string(ip_address) +
         ":" + std::to_string(config_->rpc_.port_);
}

/** Get protocol */
std::string ThalliumRpc::GetProtocol() {
  return config_->rpc_.protocol_;
}

}  // namespace hermes