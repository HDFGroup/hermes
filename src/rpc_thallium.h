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

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include "buffer_organizer.h"

namespace tl = thallium;

namespace hermes {

const int kMaxServerNamePrefix = 32; /**< max. server name prefix */
const int kMaxServerNamePostfix = 8; /**< max. server name suffix */
const char kBoPrefix[] = "BO::";     /**< buffer organizer prefix */
/** buffer organizer prefix length */
const int kBoPrefixLength = sizeof(kBoPrefix) - 1;

/**
   A structure to represent Thallium state
*/
struct ThalliumState {
  char server_name_prefix[kMaxServerNamePrefix];      /**< server prefix */
  char server_name_postfix[kMaxServerNamePostfix];    /**< server suffix */
  char bo_server_name_postfix[kMaxServerNamePostfix]; /**< buf. org. suffix */
  std::atomic<bool> kill_requested;                   /**< is kill requested? */
  tl::engine *engine;                                 /**< pointer to engine */
  tl::engine *bo_engine;        /**< pointer to buf. org. engine */
  ABT_xstream execution_stream; /**< Argobots execution stream */
};

/**
   A structure to represent a client's Thallium state
*/
struct ClientThalliumState {
  tl::engine *engine; /**< pointer to engine */
};

/**
 *  Lets Thallium know how to serialize a BufferID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param buffer_id The BufferID to serialize.
 */
template <typename A>
void serialize(A &ar, BufferID &buffer_id) {
  ar &buffer_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a BucketID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param bucket_id The BucketID to serialize.
 */
template <typename A>
void serialize(A &ar, BucketID &bucket_id) {
  ar &bucket_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a VBucketID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vbucket_id The VBucketID to serialize.
 */
template <typename A>
void serialize(A &ar, VBucketID &vbucket_id) {
  ar &vbucket_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a BlobID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param blob_id The BlobID to serialize.
 */
template <typename A>
void serialize(A &ar, BlobID &blob_id) {
  ar &blob_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a TargetID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The TargetID to serialize.
 */
template <typename A>
void serialize(A &ar, TargetID &target_id) {
  ar &target_id.as_int;
}

/** serialize \a swap_blob */
template <typename A>
void serialize(A &ar, SwapBlob &swap_blob) {
  ar &swap_blob.node_id;
  ar &swap_blob.offset;
  ar &swap_blob.size;
  ar &swap_blob.bucket_id;
}

/** serialize \a info */
template <typename A>
void serialize(A &ar, BufferInfo &info) {
  ar &info.id;
  ar &info.bandwidth_mbps;
  ar &info.size;
}

#ifndef THALLIUM_USE_CEREAL

// NOTE(chogan): Thallium's default serialization doesn't handle enums by
// default so we must write serialization code for all enums when we're not
// using cereal.

/**
 *  Lets Thallium know how to serialize a MapType.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param map_type The MapType to serialize.
 */
template <typename A>
void save(A &ar, MapType &map_type) {
  int val = (int)map_type;
  ar.write(&val, 1);
}

/**
 *  Lets Thallium know how to serialize a MapType.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param map_type The MapType to serialize.
 */
template <typename A>
void load(A &ar, MapType &map_type) {
  int val = 0;
  ar.read(&val, 1);
  map_type = (MapType)val;
}

/** save \a priority */
template <typename A>
void save(A &ar, BoPriority &priority) {
  int val = (int)priority;
  ar.write(&val, 1);
}

/** load \a priority */
template <typename A>
void load(A &ar, BoPriority &priority) {
  int val = 0;
  ar.read(&val, 1);
  priority = (BoPriority)val;
}

/** save \a violation */
template <typename A>
void save(A &ar, ThresholdViolation &violation) {
  int val = (int)violation;
  ar.write(&val, 1);
}

/** load \a violation */
template <typename A>
void load(A &ar, ThresholdViolation &violation) {
  int val = 0;
  ar.read(&val, 1);
  violation = (ThresholdViolation)val;
}
#endif  // #ifndef THALLIUM_USE_CEREAL

/** save buffer organizer \a op */
template <typename A>
void save(A &ar, BoOperation &op) {
  int val = (int)op;
  ar.write(&val, 1);
}

/** load buffer organizer \a op */
template <typename A>
void load(A &ar, BoOperation &op) {
  int val = 0;
  ar.read(&val, 1);
  op = (BoOperation)val;
}

/** serialize buffer organizer arguments */
template <typename A>
void serialize(A &ar, BoArgs &bo_args) {
  ar &bo_args.move_args.src;
  ar &bo_args.move_args.dest;
}

/** serialize buffer organizer task */
template <typename A>
void serialize(A &ar, BoTask &bo_task) {
  ar &bo_task.op;
  ar &bo_task.args;
}

/** serialize violation information */
template <typename A>
void serialize(A &ar, ViolationInfo &info) {
  ar &info.target_id;
  ar &info.violation;
  ar &info.violation_size;
}

namespace api {
template <typename A>
#ifndef THALLIUM_USE_CEREAL
void save(A &ar, api::Context &ctx) {
#else
void save(A &ar, const api::Context &ctx) {
#endif  // #ifndef THALLIUM_USE_CEREAL
  ar.write(&ctx.buffer_organizer_retries, 1);
  int val = (int)ctx.policy;
  ar.write(&val, 1);
}
template <typename A>
void load(A &ar, api::Context &ctx) {
  int val = 0;
  ar.read(&ctx.buffer_organizer_retries, 1);
  ar.read(&val, 1);
  ctx.policy = (PlacementPolicy)val;
}
}  // namespace api

std::string GetRpcAddress(RpcContext *rpc, Config *config, u32 node_id,
                          int port);
/** get Thallium state */
static inline ThalliumState *GetThalliumState(RpcContext *rpc) {
  ThalliumState *result = (ThalliumState *)rpc->state;

  return result;
}

/** get Thallium client state */
static inline ClientThalliumState *GetClientThalliumState(RpcContext *rpc) {
  ClientThalliumState *result = (ClientThalliumState *)rpc->client_rpc.state;

  return result;
}

/** is \a func_name buffer organizer function? */
static bool IsBoFunction(const char *func_name) {
  bool result = false;
  int i = 0;

  while (func_name && *func_name != '\0' && i < kBoPrefixLength) {
    if (func_name[i] != kBoPrefix[i]) {
      break;
    }
    ++i;
  }

  if (i == kBoPrefixLength) {
    result = true;
  }

  return result;
}

/** RPC call */
template <typename ReturnType, typename... Ts>
ReturnType RpcCall(RpcContext *rpc, u32 node_id, const char *func_name,
                   Ts... args) {
  VLOG(1) << "Calling " << func_name << " on node " << node_id << " from node "
          << rpc->node_id << std::endl;
  ClientThalliumState *state = GetClientThalliumState(rpc);
  bool is_bo_func = IsBoFunction(func_name);
  std::string server_name = GetServerName(rpc, node_id, is_bo_func);

  if (is_bo_func) {
    func_name += kBoPrefixLength;
  }

  tl::remote_procedure remote_proc = state->engine->define(func_name);
  // TODO(chogan): @optimization We can save a little work by storing the
  // endpoint instead of looking it up on every call
  tl::endpoint server = state->engine->lookup(server_name);

  if constexpr (std::is_same<ReturnType, void>::value) {
    remote_proc.disable_response();
    remote_proc.on(server)(std::forward<Ts>(args)...);
  } else {
    ReturnType result = remote_proc.on(server)(std::forward<Ts>(args)...);

    return result;
  }
}

}  // namespace hermes

#endif  // HERMES_RPC_THALLIUM_H_
