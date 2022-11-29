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

#include <string>
#include <vector>

#include "hermes_types.h"
#include "metadata_management.h"

namespace hermes {

struct RpcContext;

const int kMaxServerNameSize = 128;  /**< maximum size of server name */
const int kMaxServerSuffixSize = 16; /**< maximum size of server suffix */

/** start function for RPC server */
typedef void (*StartFunc)(SharedMemoryContext *, RpcContext *, Arena *,
                          const char *, int);
/**
   A structure to represent a client's RPC context.
 */
struct ClientRpcContext {
  void *state;       /**< pointer to state */
  size_t state_size; /**< size of state */
};

/**
   A structure to represent RPC context.
 */
struct RpcContext {
  ClientRpcContext client_rpc; /**< client's RPC context */
  void *state;                 /**< pointer to state*/
  /** The size of the internal RPC state. */
  size_t state_size;
  /** Array of host names stored in shared memory. This array size is
   * RpcContext::num_nodes. */
  ShmemString *host_names;
  u32 node_id;        /**< node ID */
  u32 num_nodes;      /**< number of nodes */
  int port;           /**< port number */
  bool use_host_file; /**< use host file if true */

  // TODO(chogan): Also allow reading hostnames from a file for heterogeneous or
  // non-contiguous hostnames (e.g., compute-node-20, compute-node-30,
  // storage-node-16, storage-node-24)
  // char *host_file_name;

  StartFunc start_server; /**< start function */
};

void InitRpcContext(RpcContext *rpc, u32 num_nodes, u32 node_id,
                    Config *config);
void *CreateRpcState(Arena *arena);
void InitRpcClients(RpcContext *rpc);
void ShutdownRpcClients(RpcContext *rpc);
void RunDaemon(SharedMemoryContext *context, RpcContext *rpc,
               CommunicationContext *comm, Arena *trans_arena,
               const char *shmem_name);
void FinalizeClient(SharedMemoryContext *context, RpcContext *rpc,
                    CommunicationContext *comm, Arena *trans_arena,
                    bool stop_daemon);
void FinalizeRpcContext(RpcContext *rpc, bool is_daemon);
std::string GetServerName(RpcContext *rpc, u32 node_id,
                          bool is_buffer_organizer = false);
std::string GetProtocol(RpcContext *rpc);
void StartBufferOrganizer(SharedMemoryContext *context, RpcContext *rpc,
                          const char *addr, int num_threads,
                          int port);
void StartPrefetcher(SharedMemoryContext *context,
                     RpcContext *rpc, double sleep_ms);
}  // namespace hermes

// TODO(chogan): I don't like that code similar to this is in buffer_pool.cc.
// I'd like to only have it in one place.
#if defined(HERMES_RPC_THALLIUM)
#include "rpc_thallium.h"
#else
#error "RPC implementation required (e.g., -DHERMES_RPC_THALLIUM)."
#endif

#endif  // HERMES_RPC_H_
