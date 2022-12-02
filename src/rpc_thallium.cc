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

#include "rpc.h"
#include "buffer_organizer.h"
#include "prefetcher.h"
#include "singleton.h"

namespace tl = thallium;

namespace hermes {

/** Get protocol */
std::string ThalliumRpc::GetProtocol() {
  std::string prefix = std::string(server_name_prefix);
  // NOTE(chogan): Chop "://" off the end of the server_name_prefix to get the
  // protocol
  std::string result = prefix.substr(0, prefix.length() - 3);
  return result;
}

/** initialize RPC clients */
void ThalliumRpc::InitClients() {
  std::string protocol = GetProtocol();
  // TODO(chogan): This should go in a per-client persistent arena
  client_engine_ = new tl::engine(protocol, THALLIUM_CLIENT_MODE, true, 1);
}

/** shut down RPC clients */
void ThalliumRpc::ShutdownClients() {
  if (client_engine_) {
    delete client_engine_;
    client_engine_ = 0;
  }
}

/** finalize RPC context */
void ThalliumRpc::Finalize(bool is_daemon) {
  if (is_daemon) {
    engine->wait_for_finalize();
    bo_engine->wait_for_finalize();
  } else {
    engine->finalize();
    bo_engine->finalize();
  }
  delete engine;
  delete bo_engine;
}

/** run daemon */
void ThalliumRpc::RunDaemon(const char *shmem_name) {
  engine->enable_remote_shutdown();
  bo_engine->enable_remote_shutdown();

  auto prefinalize_callback = [this, &comm = comm_]() {
    comm->SubBarrier();
    this->StopPrefetcher();
    this->StopGlobalSystemViewStateUpdateThread();
    comm->SubBarrier();
    ShutdownClients();
  };

  engine->push_prefinalize_callback(prefinalize_callback);

  bo_engine->wait_for_finalize();
  engine->wait_for_finalize();

  LocalShutdownBufferOrganizer();
  delete engine;
  delete bo_engine;

  // TODO(chogan): https://github.com/HDFGroup/hermes/issues/323
  // google::ShutdownGoogleLogging();
}

/** finalize client */
void ThalliumRpc::FinalizeClient(bool stop_daemon) {
  comm_->SubBarrier();
  if (stop_daemon && comm_->first_on_node) {
    std::string bo_server_name = GetServerName(node_id_, true);
    tl::endpoint bo_server = engine->lookup(bo_server_name);
    engine->shutdown_remote_engine(bo_server);

    std::string server_name = GetServerName(node_id_, true);
    tl::endpoint server = engine->lookup(server_name);
    engine->shutdown_remote_engine(server);
  }
  comm_->SubBarrier();
  ShutdownClients();
  // TODO(chogan): https://github.com/HDFGroup/hermes/issues/323
  // google::ShutdownGoogleLogging();
}

/** get server name */
std::string ThalliumRpc::GetServerName(u32 node_id) {
  std::string host_name = GetHostNameFromNodeId(node_id);
  // TODO(chogan): @optimization Could cache the last N hostname->IP mappings to
  // avoid excessive syscalls. Should profile first.
  struct hostent hostname_info = {};
  struct hostent *hostname_result;
  int hostname_error = 0;
  char hostname_buffer[4096] = {};
#ifdef __APPLE__
  hostname_result = gethostbyname(host_name.c_str());
      in_addr **addr_list = (struct in_addr **)hostname_result->h_addr_list;
#else
  int gethostbyname_result = gethostbyname_r(host_name.c_str(), &hostname_info,
                                             hostname_buffer, 4096,
                                             &hostname_result, &hostname_error);
  if (gethostbyname_result != 0) {
    LOG(FATAL) << hstrerror(h_errno);
  }
  in_addr **addr_list = (struct in_addr **)hostname_info.h_addr_list;
#endif
  if (!addr_list[0]) {
    LOG(FATAL) << hstrerror(h_errno);
  }

  char ip_address[INET_ADDRSTRLEN];
  const char *inet_result = inet_ntop(AF_INET,
                                      addr_list[0],
                                      ip_address,
                                      INET_ADDRSTRLEN);
  if (!inet_result) {
    FailedLibraryCall("inet_ntop");
  }

  std::string result = std::string(server_name_prefix);
  result += std::string(ip_address);

  if (is_buffer_organizer) {
    result += std::string(bo_server_name_postfix);
  } else {
    result += std::string(server_name_postfix);
  }

  return result;
}

/** start Thallium RPC server */
void ThalliumRpc::StartServer(const char *addr,
                              i32 num_rpc_threads) {
  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
}

}  // namespace hermes