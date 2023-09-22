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
#include <string>
#include "hermes_shm/util/singleton.h"

#include "rpc.h"

namespace tl = thallium;

namespace labstor {

/**
   A structure to represent Thallium state
*/
class ThalliumRpc {
 public:
  std::atomic<bool> kill_requested_; /**< is kill requested? */
  std::unique_ptr<tl::engine> client_engine_; /**< pointer to client engine */
  std::unique_ptr<tl::engine> server_engine_; /**< pointer to server engine */
  RpcContext *rpc_;

  /** initialize RPC context  */
  ThalliumRpc() {}

  /** Initialize server */
  void ServerInit(RpcContext *rpc) {
    rpc_ = rpc;
    HILOG(kInfo, "Initializing RPC server");
    std::string addr = rpc->GetMyRpcAddress();
    HILOG(kInfo, "Attempting to start server on: {}", addr);
    try {
      server_engine_ = std::make_unique<tl::engine>(
          addr, THALLIUM_SERVER_MODE, true, rpc->num_threads_);
    } catch (std::exception &e) {
      HELOG(kFatal, "RPC init failed for host: {}\n{}", addr, e.what());
    }
    std::string rpc_server_name = server_engine_->self();
    HILOG(kInfo, "Serving {} (i.e., {}) with {} RPC threads as node id {}",
          rpc_server_name,
          addr,
          rpc->num_threads_,
          rpc->node_id_);
    ClientInit(rpc);
  }

  /** Initialize client */
  void ClientInit(RpcContext *rpc) {
    rpc_ = rpc;
    std::string protocol = rpc->GetProtocol();
    client_engine_ = std::make_unique<tl::engine>(protocol,
                                                  THALLIUM_CLIENT_MODE,
                                                  true, 1);
    HILOG(kInfo, "This client is on node {} (i.e., {}, proto: {})",
          rpc->node_id_, rpc->GetHostNameFromNodeId(DomainId::GetNode(rpc->node_id_)), protocol);
  }

  /** Run the daemon */
  void RunDaemon() {
    HILOG(kInfo, "Starting the daemon on node: {}", rpc_->node_id_);
    server_engine_->enable_remote_shutdown();
    auto prefinalize_callback = [this]() {
      HILOG(kInfo, "Finalizing RPCs on node: {}", rpc_->node_id_);
    };
    server_engine_->push_prefinalize_callback(prefinalize_callback);
    server_engine_->wait_for_finalize();
    HILOG(kInfo, "Daemon has stopped on node: {}", rpc_->node_id_);
  }

  /** Stop this daemon */
  void StopThisDaemon() {
    StopDaemon(rpc_->node_id_);
  }

  /** Stop a thallium daemon */
  void StopDaemon(u32 node_id) {
    try {
      HILOG(kInfo, "Sending stop signal to: {}", node_id);
      std::string server_name = GetServerName(node_id);
      tl::endpoint server = client_engine_->lookup(server_name.c_str());
      client_engine_->shutdown_remote_engine(server);
    } catch (std::exception &e) {
      HELOG(kFatal, e.what());
    }
  }

  /** Stop the thallium daemon */
  void StopAllDaemons() {
    for (u32 node_id = 1; node_id < (int)rpc_->hosts_.size() + 1; ++node_id) {
      StopDaemon(node_id);
    }
  }

  /** Thallium-compatible server name */
  std::string GetServerName(u32 node_id) {
    std::string ip_address = rpc_->GetIpAddressFromNodeId(DomainId::GetNode(node_id));
    return rpc_->protocol_ + "://" +
        std::string(ip_address) +
        ":" + std::to_string(rpc_->port_);
  }

  /** Register an RPC with thallium */
  template<typename RpcLambda>
  void RegisterRpc(const char *name, RpcLambda &&lambda) {
    server_engine_->define(name, std::forward<RpcLambda>(lambda));
  }

  /** RPC call */
  template <typename RetT, bool ASYNC, typename... Args>
  RetT Call(u32 node_id, const char *func_name, Args&&... args) {
    HILOG(kDebug, "Calling {} {} -> {}", func_name, rpc_->node_id_, node_id)
    try {
      std::string server_name = GetServerName(node_id);
      tl::remote_procedure remote_proc = client_engine_->define(func_name);
      tl::endpoint server = client_engine_->lookup(server_name);
      HILOG(kDebug, "Found the server: {}={}", node_id, server_name)
      if constexpr(!ASYNC) {
        if constexpr (std::is_same<RetT, void>::value) {
          remote_proc.disable_response();
          remote_proc.on(server)(std::forward<Args>(args)...);
        } else {
          RetT result = remote_proc.on(server)(std::forward<Args>(args)...);
          return result;
        }
      } else {
        return remote_proc.on(server).async(std::forward<Args>(args)...);
      }
    } catch (tl::margo_exception &err) {
      HELOG(kFatal, "(node {} -> {}) Thallium failed on function: {}: {}",
            rpc_->node_id_, node_id, func_name, err.what())
      exit(1);
    }
  }

  /** RPC call */
  template <typename RetT, typename... Args>
  RetT SyncCall(u32 node_id, const char *func_name, Args&&... args) {
    return Call<RetT, false>(
        node_id, func_name, std::forward<Args>(args)...);
  }

  /** Async RPC call */
  template <typename... Args>
  thallium::async_response AsyncCall(u32 node_id, const char *func_name, Args&&... args) {
    return Call<thallium::async_response, true>(
        node_id, func_name, std::forward<Args>(args)...);
  }

  /** I/O transfers */
  template<typename RetT, bool ASYNC, typename ...Args>
  RetT IoCall(i32 node_id, const char *func_name,
              IoType type, char *data, size_t size, Args&& ...args) {
    HILOG(kDebug, "Calling {} {} -> {}", func_name, rpc_->node_id_, node_id)
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
    if constexpr (!ASYNC) {
      if constexpr (std::is_same_v<RetT, void>) {
        remote_proc.on(server)(bulk, std::forward<Args>(args)...);
      } else {
        return remote_proc.on(server)(bulk, std::forward<Args>(args)...);
      }
    } else {
      return remote_proc.on(server).async(bulk, std::forward<Args>(args)...);
    }
  }

  /** Synchronous I/O transfer */
  template<typename RetT, typename ...Args>
  RetT SyncIoCall(i32 node_id, const char *func_name,
                  IoType type, char *data, size_t size, Args&& ...args) {
    return IoCall<RetT, false>(
        node_id, func_name, type, data, size, std::forward<Args>(args)...);
  }

  /** I/O transfers */
  template<typename ...Args>
  thallium::async_response AsyncIoCall(u32 node_id, const char *func_name,
                                       IoType type, char *data, size_t size,
                                       Args&& ...args) {
    return IoCall<thallium::async_response, true>(
        node_id, func_name, type, data, size, std::forward<Args>(args)...);
  }

  /** Io transfer at the server */
  size_t IoCallServer(const tl::request &req, const tl::bulk &bulk,
                      IoType type, char *data, size_t size) {
    tl::bulk_mode flag = tl::bulk_mode::write_only;
    switch (type) {
      case IoType::kRead: {
        // The "local_bulk" object will only be read from
        flag = tl::bulk_mode::read_only;
        // flag = tl::bulk_mode::read_write;
        HILOG(kInfo, "(node {}) Reading {} bytes from the server",
              rpc_->node_id_, size)
        break;
      }
      case IoType::kWrite: {
        // The "local_bulk" object will only be written to
        flag = tl::bulk_mode::write_only;
        // flag = tl::bulk_mode::read_write;
        HILOG(kInfo, "(node {}) Writing {} bytes to the server",
              rpc_->node_id_, size)
        break;
      }
      default: {
        // NOTE(llogan): Avoids "uninitalized" warning
        HELOG(kFatal, "Cannot have none I/O type")
      }
    }

    tl::endpoint endpoint = req.get_endpoint();
    std::vector<std::pair<void*, size_t>> segments(1);
    segments[0].first  = data;
    segments[0].second = size;
    tl::bulk local_bulk = server_engine_->expose(segments, flag);
    size_t io_bytes = 0;

    try {
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
        }
      }
    } catch (std::exception &e) {
      HELOG(kFatal, "(node {}) Failed to perform bulk I/O thallium: {} (type={})",
            rpc_->node_id_, e.what(), (type==IoType::kRead)?"read":"write");
    }
    if (io_bytes != size) {
      HELOG(kFatal, "Failed to perform bulk I/O thallium")
    }
    return io_bytes;
  }

  /** Check if request is complete */
  bool IsDone(thallium::async_response &req) {
    return req.received();
  }

  /** Wait for thallium to complete */
  template<typename RetT>
  RetT Wait(thallium::async_response &req) {
    if constexpr(std::is_same_v<void, RetT>) {
      req.wait().as<RetT>();
    } else {
      return req.wait();
    }
  }
};

}  // namespace hermes

/** Lets thallium know how to serialize an enum */
#define SERIALIZE_ENUM(T)\
  template <typename A>\
  void save(A &ar, T &mode) {\
    int cast = static_cast<int>(mode);\
    ar << cast;\
  }\
  template <typename A>\
  void load(A &ar, T &mode) {\
    int cast;\
    ar >> cast;\
    mode = static_cast<T>(cast);\
  }

#endif  // HERMES_RPC_THALLIUM_H_
