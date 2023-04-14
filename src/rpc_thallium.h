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
#include "hermes_shm/data_structures/serialization/thallium.h"

#include "rpc.h"

namespace tl = thallium;

namespace hermes {

/**
   A structure to represent Thallium state
*/
class ThalliumRpc  : public RpcContext {
 public:
  std::atomic<bool> kill_requested_; /**< is kill requested? */
  std::unique_ptr<tl::engine> client_engine_; /**< pointer to client engine */
  std::unique_ptr<tl::engine> server_engine_; /**< pointer to server engine */

  /** initialize RPC context  */
  ThalliumRpc() : RpcContext() {}

  void InitServer() override;
  void InitClient() override;
  void Finalize();
  void RunDaemon();
  void StopDaemon();
  std::string GetServerName(i32 node_id);

  template<typename RpcLambda>
  void RegisterRpc(const char *name, RpcLambda &&lambda) {
    server_engine_->define(name, std::forward<RpcLambda>(lambda));
  }

  /** RPC call */
  template <typename ReturnType, typename... Args>
  ReturnType Call(i32 node_id, const char *func_name, Args&&... args) {
    HILOG(kDebug, "Calling {} {} -> {}", func_name, node_id_, node_id)
    try {
      std::string server_name = GetServerName(node_id);
      tl::remote_procedure remote_proc = client_engine_->define(func_name);
      tl::endpoint server = client_engine_->lookup(server_name);
      HILOG(kDebug, "Found the server: {}", server_name)
      if constexpr (std::is_same<ReturnType, void>::value) {
        remote_proc.disable_response();
        remote_proc.on(server)(std::forward<Args>(args)...);
      } else {
        ReturnType result = remote_proc.on(server)(std::forward<Args>(args)...);
        return result;
      }
    } catch (tl::margo_exception &err) {
      HELOG(kFatal, "Thallium failed on function: {}\n{}",
            func_name, err.what())
      exit(1);
    }
  }

  /** I/O transfers */
  template<typename ReturnType, typename ...Args>
  ReturnType IoCall(i32 node_id, const char *func_name,
                    IoType type, char *data, size_t size, Args&& ...args) {
    HILOG(kDebug, "Calling {} {} -> {}", func_name, node_id_, node_id)
    std::string server_name = GetServerName(node_id);
    tl::bulk_mode flag;
    switch (type) {
      case IoType::kRead: {
        // The "bulk" object will be modified
        flag = tl::bulk_mode::write_only;
        break;
      }
      case IoType::kWrite: {
        // The "bulk" object will only be read from
        flag = tl::bulk_mode::read_only;
        break;
      }
      case IoType::kNone: {
        // TODO(llogan)
        HELOG(kFatal, "Cannot have none I/O type")
        exit(1);
      }
    }

    tl::remote_procedure remote_proc = client_engine_->define(func_name);
    tl::endpoint server = client_engine_->lookup(server_name);

    std::vector<std::pair<void*, size_t>> segments(1);
    segments[0].first  = data;
    segments[0].second = size;

    tl::bulk bulk = client_engine_->expose(segments, flag);
    if constexpr(std::is_same_v<ReturnType, void>) {
      remote_proc.on(server)(bulk, std::forward<Args>(args)...);
    } else {
      return remote_proc.on(server)(bulk, std::forward<Args>(args)...);
    }
  }

  /** Io transfer at the server */
  size_t IoCallServer(const tl::request &req, tl::bulk &bulk,
                      IoType type, char *data, size_t size) {
    tl::bulk_mode flag;
    switch (type) {
      case IoType::kRead: {
        // The "local_bulk" object will only be read from
        flag = tl::bulk_mode::read_only;
        break;
      }
      case IoType::kWrite: {
        // The "local_bulk" object will only be written to
        flag = tl::bulk_mode::write_only;
        break;
      }
      default: {
        // NOTE(llogan): Avoids "uninitalized" warning
        flag = tl::bulk_mode::write_only;
        HELOG(kFatal, "Cannot have none I/O type")
      }
    }

    tl::endpoint endpoint = req.get_endpoint();
    std::vector<std::pair<void*, size_t>> segments(1);
    segments[0].first  = data;
    segments[0].second = size;
    tl::bulk local_bulk = server_engine_->expose(segments, flag);
    size_t io_bytes;

    switch (type) {
      case IoType::kRead: {
        // Read from "local_bulk" to "bulk"
        io_bytes = bulk.on(endpoint) << local_bulk;
        break;
      }
      case IoType::kWrite: {
        // Write to "local_bulk" from "bulk"
        io_bytes = bulk.on(endpoint) >> local_bulk;
        break;
      }
      case IoType::kNone: {
        HELOG(kFatal, "Cannot have none I/O type")
        exit(1);
      }
    }

    // TODO(llogan): @errorhandling
    assert(io_bytes == size);
    return io_bytes;
  }

 private:
  void DefineRpcs();
};

}  // namespace hermes

#endif  // HERMES_RPC_THALLIUM_H_
