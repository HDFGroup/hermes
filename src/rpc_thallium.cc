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
  // Load the host names

  DefineRpcs();
}

/** initialize RPC clients */
void ThalliumRpc::InitClients() {
  std::string protocol = GetProtocol();
  // TODO(chogan): This should go in a per-client persistent arena
  client_engine_ = tl::engine(protocol,
                              THALLIUM_CLIENT_MODE,
                              true, 1);
}

/** finalize RPC context */
void ThalliumRpc::Finalize() {
}

/** run daemon */
void ThalliumRpc::RunDaemon(const char *shmem_name) {
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

  result = config_.prefix_ std::string(ip_address);

  return result;
}

/** Get protocol */
std::string ThalliumRpc::GetProtocol() {
  std::string prefix = std::string(server_name_prefix);
  // NOTE(chogan): Chop "://" off the end of the server_name_prefix to get the
  // protocol
  std::string result = prefix.substr(0, prefix.length() - 3);
  return result;
}

}  // namespace hermes