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

#include "hermes.h"
#include "metadata_manager.h"
#include "rpc_thallium.h"
#include "hermes_shm/util/singleton.h"
#include "prefetcher.h"
#include <fstream>

namespace tl = thallium;

namespace hermes {

/** start Thallium RPC server */
void ThalliumRpc::InitServer() {
  HILOG(kInfo, "Initializing RPC server");
  InitRpcContext();
  std::string addr = GetMyRpcAddress();
  HILOG(kInfo, "Attempting to start server on: {}", addr);
  try {
    server_engine_ = std::make_unique<tl::engine>(
        addr, THALLIUM_SERVER_MODE, true, config_->rpc_.num_threads_);
  } catch (std::exception &e) {
    HELOG(kFatal, "RPC init failed for host: {}\n{}", addr, e.what());
  }
  std::string rpc_server_name = server_engine_->self();
  HILOG(kInfo, "Serving {} (i.e., {}) with {} RPC threads as node id {}",
        rpc_server_name, addr,
        config_->rpc_.num_threads_,
        node_id_);
  DefineRpcs();
}

/** initialize RPC clients */
void ThalliumRpc::InitClient()  {
  InitRpcContext();
  std::string protocol = GetProtocol();
  client_engine_ = std::make_unique<tl::engine>(protocol,
                              THALLIUM_CLIENT_MODE,
                              true, 1);
  HILOG(kInfo, "This client is on node {} (i.e., {})",
        node_id_, GetHostNameFromNodeId(node_id_));
}

/** run daemon */
void ThalliumRpc::RunDaemon() {
  server_engine_->enable_remote_shutdown();
  auto prefinalize_callback = [this]() {
    HILOG(kInfo, "Beginning finalization on node: {}", this->node_id_);
    this->Finalize();
    HILOG(kInfo, "Finished finalization on node: {}", this->node_id_);
  };

  // TODO(llogan): add config param to do this
  std::ofstream daemon_started_fs;
  daemon_started_fs.open("/tmp/hermes_daemon_log.txt");
  daemon_started_fs << HERMES_SYSTEM_INFO->pid_;
  daemon_started_fs.close();
  HILOG(kInfo, "Running the daemon on node: {}", node_id_);

  server_engine_->push_prefinalize_callback(prefinalize_callback);
  server_engine_->wait_for_finalize();
  HILOG(kInfo, "Daemon has stopped on node: {}", node_id_);
}

/** stop daemon (from client) */
void ThalliumRpc::StopDaemon() {
  try {
    for (i32 node_id = 1; node_id < (int)hosts_.size() + 1; ++node_id) {
      HILOG(kInfo, "Sending stop signal to: {}", node_id);
      std::string server_name = GetServerName(node_id);
      tl::endpoint server = client_engine_->lookup(server_name.c_str());
      client_engine_->shutdown_remote_engine(server);
    }
  } catch (std::exception &e) {
    HELOG(kFatal, e.what());
  }
}

/** get server name */
std::string ThalliumRpc::GetServerName(i32 node_id) {
  std::string ip_address = GetIpAddressFromNodeId(node_id);
  return config_->rpc_.protocol_ + "://" +
         std::string(ip_address) +
         ":" + std::to_string(config_->rpc_.port_);
}

/** finalize RPC context */
void ThalliumRpc::Finalize() {
  switch (mode_) {
    case HermesType::kServer: {
      HILOG(kInfo, "Stopping (server mode)");
      HERMES_THREAD_MANAGER->Join();
      HERMES->prefetch_.Finalize();
      // NOTE(llogan): Don't use finalize with unique_ptr. finalize() is
      // called in the destructor of the tl::enigne, and will segfault if
      // called twice.

      // server_engine_->finalize()
      // client_engine_->finalize()
      break;
    }
    case HermesType::kClient: {
      HILOG(kInfo, "Stopping (client mode)");
      HERMES_THREAD_MANAGER->Join();
      // NOTE(llogan): Don't use finalize with unique_ptr. finalize() is
      // called in the destructor of the tl::enigne, and will segfault if
      // called twice.

      // client_engine_.release();
      break;
    }
    default: {
      throw std::logic_error("Invalid Hermes initialization type");
    }
  }
}

}  // namespace hermes
