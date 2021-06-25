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

const int kMaxServerNameSize = 128;
const int kMaxServerSuffixSize = 16;

typedef void (*StartFunc)(SharedMemoryContext*, RpcContext*, Arena*,
                          const char*, int);

struct ClientRpcContext {
  void *state;
  size_t state_size;
};

struct RpcContext {
  ClientRpcContext client_rpc;
  void *state;
  /** The size of the internal rpc state. */
  size_t state_size;
  u32 node_id;
  u32 num_nodes;
  int port;
  /** The first and last host numbers. This is a convenient way of specifiying
   * multiple hostnames if the pattern is predictable (i.e., if running on nodes
   * with names cluster-node-6, cluster-node-7, ... to cluster-node-32, the
   * host_number_range would be {6, 32} and Hermes can generate all names). */
  u32 host_number_range[2];
  /** The host name without the host number. Allows programmatic construction of
   * predictable host names like cluster-node-1, cluster-node-2, etc. without
   * storing extra copies of the base hostname.*/
  char base_hostname[kMaxServerNameSize];

  char hostname_suffix[kMaxServerSuffixSize];

  // TODO(chogan): Also allow reading hostnames from a file for heterogeneous or
  // non-contiguous hostnames (e.g., compute-node-20, compute-node-30,
  // storage-node-16, storage-node-24)
  // char *host_file_name;

  StartFunc start_server;
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
std::string GetHostNumberAsString(RpcContext *rpc, u32 node_id);
std::string GetServerName(RpcContext *rpc, u32 node_id,
                          bool is_buffer_organizer = false);
std::string GetProtocol(RpcContext *rpc);
void StartBufferOrganizer(SharedMemoryContext *context, RpcContext *rpc,
                          Arena *arena, const char *addr, int num_threads,
                          int port);
#if 0
void TriggerBufferOrganizer(RpcContext *rpc, const char *func_name,
                            const std::string &blob_name, SwapBlob swap_blob,
                            int retries);
#endif
}  // namespace hermes

// TODO(chogan): I don't like that code similar to this is in buffer_pool.cc.
// I'd like to only have it in one place.
#if defined(HERMES_RPC_THALLIUM)
#include "rpc_thallium.h"
#else
#error "RPC implementation required (e.g., -DHERMES_RPC_THALLIUM)."
#endif

#endif  // HERMES_RPC_H_
