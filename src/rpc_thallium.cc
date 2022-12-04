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
  InitHostInfo();
  std::string addr = GetMyRpcAddress();
  server_engine_ = std::make_unique<tl::engine>(addr,
                              THALLIUM_SERVER_MODE,
                              true,
                              config_->rpc_.num_threads_);
  std::string rpc_server_name = server_engine_->self();
  LOG(INFO) << "Serving at " << rpc_server_name << " with "
            << config_->rpc_.num_threads_ << " RPC threads" << std::endl;
  DefineRpcs();
}

/** initialize RPC clients */
void ThalliumRpc::InitClient() {
  InitHostInfo();
  std::string protocol = GetProtocol();
  client_engine_ = std::make_unique<tl::engine>(protocol,
                              THALLIUM_CLIENT_MODE,
                              true, 1);
}

/** initialize RPC for colocated mode */
void ThalliumRpc::InitColocated() {
  InitHostInfo();
  // NOTE(llogan): RPCs are never used in colocated mode.
}

/** run daemon */
void ThalliumRpc::RunDaemon() {
  server_engine_->enable_remote_shutdown();
  auto prefinalize_callback = [this]() {
    Finalize();
  };
  server_engine_->push_prefinalize_callback(prefinalize_callback);
  server_engine_->wait_for_finalize();
  client_engine_->finalize();
}

/** stop daemon (from client) */
void ThalliumRpc::StopDaemon() {
  std::string server_name = GetServerName(node_id_);
  tl::endpoint server = client_engine_->lookup(server_name.c_str());
  client_engine_->shutdown_remote_engine(server);
}

/** get server name */
std::string ThalliumRpc::GetServerName(u32 node_id) {
  std::string ip_address = GetIpAddressFromNodeId(node_id);
  return config_->rpc_.protocol_ + "://" +
         std::string(ip_address) +
         ":" + std::to_string(config_->rpc_.port_);
}

/** finalize RPC context */
void ThalliumRpc::Finalize() {
  switch (comm_->type_) {
    case HermesType::kServer: {
      comm_->WorldBarrier();
      this->kill_requested_.store(true);
      break;
    }
    case HermesType::kClient: {
      client_engine_->finalize();
      break;
    }
    case HermesType::kColocated: {
      //TODO(llogan)
      break;
    }
  }
}

}  // namespace hermes