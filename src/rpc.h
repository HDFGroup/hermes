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
#include "decorator.h"
#include "config_server.h"
#include "utils.h"

namespace hermes::api {
class Hermes;
}  // namespace hermes::api

namespace hermes {

struct MetadataManager;
using api::Hermes;

/** RPC types */
enum class RpcType {
  kThallium
};

/** Uniquely identify a host machine */
struct HostInfo {
  int node_id_;
  std::string hostname_;
  std::string ip_addr_;

  HostInfo() = default;
  explicit HostInfo(const std::string &hostname, const std::string &ip_addr)
      : hostname_(hostname), ip_addr_(ip_addr) {}
};

/** A structure to represent RPC context. */
class RpcContext {
 public:
  COMM_TYPE *comm_;
  ServerConfig *config_;
  MetadataManager *mdm_;
  int port_;  /**< port number */
  int node_id_; /**< the ID of this node*/
  std::vector<HostInfo> hosts_; /**< Hostname and ip addr per-node */
  HermesType mode_; /**< The current mode hermes is executing in */

 public:
  RpcContext() = default;

  /** Parse a hostfile */
  static std::vector<std::string> ParseHostfile(const std::string &path);

  /** initialize host info list */
  void InitRpcContext();

  /** Check if we should skip an RPC and call a function locally */
  bool ShouldDoLocalCall(int node_id);

  /** get RPC address */
  std::string GetRpcAddress(u32 node_id, int port);

  /** Get RPC address for this node */
  std::string GetMyRpcAddress();

  /** get host name from node ID */
  std::string GetHostNameFromNodeId(u32 node_id);

  /** get host name from node ID */
  std::string GetIpAddressFromNodeId(u32 node_id);

  /** Get RPC protocol */
  std::string GetProtocol();

 private:
  /** Get the IPv4 address of this machine */
  std::string _GetMyIpAddress();

  /** Get IPv4 address from the host with "host_name" */
  std::string _GetIpAddress(const std::string &host_name);

 public:
  virtual void InitServer() = 0;
  virtual void InitClient() = 0;
};

}  // namespace hermes


#define UNIQUE_ID_TO_NODE_ID_LAMBDA \
  [](auto &&param) { return param.GetNodeId(); }

#ifdef HERMES_ONLY_RPC
#define NODE_ID_IS_LOCAL(node_id) false
#else
#define NODE_ID_IS_LOCAL(node_id) (node_id) == (rpc_->node_id_)
#endif

#define DEFINE_RPC(RET, BaseName, tuple_idx, hashfn)\
  template<typename ...Args>\
  TYPE_UNWRAP(RET) Global##BaseName(Args&& ...args) {\
    if constexpr(std::is_same_v<TYPE_UNWRAP(RET), void>) {\
      _Global##BaseName(\
          hermes_shm::make_argpack(std::forward<Args>(args)...));\
    } else {\
      return _Global##BaseName(\
          hermes_shm::make_argpack(std::forward<Args>(args)...));\
    }\
  }\
  template<typename ArgPackT>\
  TYPE_UNWRAP(RET) _Global##BaseName(ArgPackT &&pack) {\
    int node_id = hashfn(pack.template              \
                         Forward<tuple_idx>()) % rpc_->hosts_.size(); \
    node_id += 1; \
    if (NODE_ID_IS_LOCAL(node_id)) {\
      if constexpr(std::is_same_v<TYPE_UNWRAP(RET), void>) {\
        hermes_shm::PassArgPack::Call(\
            std::forward<ArgPackT>(pack), \
            [this](auto &&...args) constexpr {\
              this->Local##BaseName(std::forward<decltype(args)>(args)...);\
            });\
      } else {\
        return hermes_shm::PassArgPack::Call(\
            std::forward<ArgPackT>(pack), \
            [this](auto &&...args) constexpr {\
              return this->Local##BaseName( \
                  std::forward<decltype(args)>(args)...);\
            });\
      } \
    } else { \
      return hermes_shm::PassArgPack::Call( \
          std::forward<ArgPackT>(pack), \
          [this, node_id](auto&& ...args) constexpr { \
            if constexpr(std::is_same_v<TYPE_UNWRAP(RET), void>) { \
              this->rpc_->Call<TYPE_UNWRAP(RET)>( \
                  node_id, "Rpc" #BaseName, \
                  std::forward<decltype(args)>(args)...); \
            } else { \
              return this->rpc_->Call<TYPE_UNWRAP(RET)>( \
                  node_id, "Rpc" #BaseName, \
                  std::forward<decltype(args)>(args)...); \
            } \
          }); \
    } \
  }
#include "rpc_factory.h"

#endif  // HERMES_RPC_H_
