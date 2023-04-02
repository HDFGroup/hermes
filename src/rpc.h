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
#include "thread_manager.h"

namespace hermes::api {
class Hermes;
}  // namespace hermes::api

namespace hermes {

class MetadataManager;
using api::Hermes;

/** RPC types */
enum class RpcType {
  kThallium
};

/** Uniquely identify a host machine */
struct HostInfo {
  i32 node_id_;
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
  i32 node_id_; /**< the ID of this node*/
  std::vector<HostInfo> hosts_; /**< Hostname and ip addr per-node */
  HermesType mode_; /**< The current mode hermes is executing in */

 public:
  RpcContext() = default;

  /** Parse a hostfile */
  static std::vector<std::string> ParseHostfile(const std::string &path);

  /** initialize host info list */
  void InitRpcContext();

  /** Check if we should skip an RPC and call a function locally */
  bool ShouldDoLocalCall(i32 node_id);

  /** get RPC address */
  std::string GetRpcAddress(i32 node_id, int port);

  /** Get RPC address for this node */
  std::string GetMyRpcAddress();

  /** get host name from node ID */
  std::string GetHostNameFromNodeId(i32 node_id);

  /** get host name from node ID */
  std::string GetIpAddressFromNodeId(i32 node_id);

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


/** Decide the node to send an RPC based on a UniqueId template */
#define UNIQUE_ID_TO_NODE_ID_LAMBDA \
  [](auto &&param) { return param.GetNodeId(); }

/** Decide the node to send an RPC based on a std::string */
#define STRING_HASH_LAMBDA \
  [this](auto &&param) {   \
    auto hash = std::hash<std::string>{}(param); \
    hash %= this->rpc_->hosts_.size(); \
    return hash + 1; \
  }

/** Decide the node to send an RPC based on serialized trait parameters */
#define TRAIT_PARAMS_HASH_LAMBDA \
  [this](auto &&param) { \
      TraitHeader *hdr = reinterpret_cast<TraitHeader*>(param.data()); \
      std::string trait_uuid = hdr->trait_uuid_; \
      auto hash = std::hash<std::string>{}(trait_uuid); \
      hash %= this->rpc_->hosts_.size(); \
      return hash + 1; \
    }

/** A helper to test RPCs in hermes */
#ifdef HERMES_ONLY_RPC
#define NODE_ID_IS_LOCAL(node_id) false
#else
#define NODE_ID_IS_LOCAL(node_id) (node_id) == (rpc_->node_id_)
#endif

/**
 * For a function which need to be called as an RPC,
 * this will create the global form of that function.
 *
 * E.g., let's say you have a function:
 * BlobId LocalGetBlobId(std::string name)
 *
 * @param RET the return value of the RPC
 * @param BaseName the base of the function name. E.g., GetBlobId()
 * @param tuple_idx the offset of the parameter in the function prototype to
 * determine where to send the RPC. E.g., 0 -> std::string name
 * @param hashfn the hash function used to actually compute the node id.
 * E.g., STRING_HASH_LAMBDA
 * */
#define DEFINE_RPC(RET, BaseName, tuple_idx, hashfn)\
  template<typename ...Args>\
  TYPE_UNWRAP(RET) Global##BaseName(Args&& ...args) {\
    if constexpr(std::is_same_v<TYPE_UNWRAP(RET), void>) {\
      _Global##BaseName(\
          hshm::make_argpack(std::forward<Args>(args)...));\
    } else {\
      return _Global##BaseName(\
          hshm::make_argpack(std::forward<Args>(args)...));\
    }\
  }\
  template<typename ArgPackT>\
  TYPE_UNWRAP(RET) _Global##BaseName(ArgPackT &&pack) {\
    i32 node_id = hashfn(pack.template              \
                         Forward<tuple_idx>()); \
    if (NODE_ID_IS_LOCAL(node_id)) {\
      if constexpr(std::is_same_v<TYPE_UNWRAP(RET), void>) {\
        hshm::PassArgPack::Call(\
            std::forward<ArgPackT>(pack), \
            [this](auto &&...args) constexpr {\
              this->Local##BaseName(std::forward<decltype(args)>(args)...);\
            });\
      } else {\
        return hshm::PassArgPack::Call(\
            std::forward<ArgPackT>(pack), \
            [this](auto &&...args) constexpr {\
              return this->Local##BaseName( \
                  std::forward<decltype(args)>(args)...);\
            });\
      } \
    } else { \
      return hshm::PassArgPack::Call( \
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
