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

#ifndef HERMES_RPC_THALLIUM_H_
#define HERMES_RPC_THALLIUM_H_

#include <thallium.hpp>
#include "rpc_thallium_serialization.h"
#include "communication.h"
#include "config.h"
#include "utils.h"

#include "rpc.h"

namespace tl = thallium;

namespace hermes {

/**
   A structure to represent Thallium state
*/
class ThalliumRpc : public RpcContext {
 public:
  std::atomic<bool> kill_requested_; /**< is kill requested? */
  tl::engine server_engine_;         /**< pointer to engine */
  tl::engine bo_engine_;             /**< pointer to buf. org. engine */
  ABT_xstream execution_stream_;     /**< Argobots execution stream */
  tl::engine client_engine_;         /**< pointer to engine */
  tl::engine io_engine_;         /**< pointer to engine */

  /** initialize RPC context  */
  explicit ThalliumRpc(COMM_TYPE *comm, ServerConfig *config)
      : RpcContext(comm, config) {}

  void InitClient() override;
  void InitServer() override;
  void Finalize();
  void RunDaemon();
  std::string GetServerName(u32 node_id);
  std::string GetProtocol();

  /** RPC call */
  template <typename ReturnType, typename... Ts>
  ReturnType Call(u32 node_id, const char *func_name, Ts... args) {
    VLOG(1) << "Calling " << func_name << " on node " << node_id
            << " from node " << node_id << std::endl;
    std::string server_name = GetServerName(node_id);
    tl::remote_procedure remote_proc = client_engine_.define(func_name);
    tl::endpoint server = client_engine_.lookup(server_name);
    if constexpr (std::is_same<ReturnType, void>::value) {
      remote_proc.disable_response();
      remote_proc.on(server)(std::forward<Ts>(args)...);
    } else {
      ReturnType result = remote_proc.on(server)(std::forward<Ts>(args)...);
      return result;
    }
  }

  /** I/O transfers */
  size_t IoCall(u32 node_id, IoType type, u8 *data,
                TargetID id, size_t off, size_t size) {
    std::string server_name = GetServerName(node_id);
    const char *func_name;
    tl::bulk_mode flag;

    switch (type) {
      case IoType::kRead: {
        func_name = "BulkRead";
        flag = tl::bulk_mode::read_only;
      }
      case IoType::kWrite: {
        func_name = "BulkWrite";
        flag = tl::bulk_mode::write_only;
      }
    }

    tl::remote_procedure remote_proc = io_engine_.define(func_name);
    tl::endpoint server = io_engine_.lookup(server_name);

    std::vector<std::pair<void*, size_t>> segments(1);
    segments[0].first  = data;
    segments[0].second = size;

    tl::bulk bulk = io_engine_.expose(segments, flag);
    // size_t result = remote_proc.on(server)(bulk, id);
    size_t result = remote_proc.on(server)(bulk);

    return result;
  }

 private:
  void DefineRpcs();
};

}  // namespace hermes

#endif  // HERMES_RPC_THALLIUM_H_
