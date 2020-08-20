#ifndef HERMES_COMMON_H_
#define HERMES_COMMON_H_

#include <sys/mman.h>
#include <sys/stat.h>
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <cmath>
#include <memory>

#include <mpi.h>

#include "hermes.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "metadata_management.h"
#include "test_utils.h"

/**
 * @file common.h
 *
 * Common configuration and utilities for tests.
 */

namespace hermes {

const char mem_mount_point[] = "";
const char nvme_mount_point[] = "./";
const char bb_mount_point[] = "./";
const char pfs_mount_point[] = "./";
const char buffer_pool_shmem_name[] = "/hermes_buffer_pool_";
const char rpc_server_name[] = "sockets://localhost:8080";

void InitTestConfig(Config *config) {
  config->num_tiers = 4;
  assert(config->num_tiers < kMaxTiers);

  for (int tier = 0; tier < config->num_tiers; ++tier) {
    config->capacities[tier] = MEGABYTES(50);
    config->block_sizes[tier] = KILOBYTES(4);
    config->num_slabs[tier] = 4;

    config->slab_unit_sizes[tier][0] = 1;
    config->slab_unit_sizes[tier][1] = 4;
    config->slab_unit_sizes[tier][2] = 16;
    config->slab_unit_sizes[tier][3] = 32;

    config->desired_slab_percentages[tier][0] = 0.25f;
    config->desired_slab_percentages[tier][1] = 0.25f;
    config->desired_slab_percentages[tier][2] = 0.25f;
    config->desired_slab_percentages[tier][3] = 0.25f;
  }

  config->bandwidths[0] = 6000.0f;
  config->bandwidths[1] = 300.f;
  config->bandwidths[2] = 150.0f;
  config->bandwidths[3] = 70.0f;

  config->latencies[0] = 15.0f;
  config->latencies[1] = 250000;
  config->latencies[2] = 500000.0f;
  config->latencies[3] = 1000000.0f;

  // TODO(chogan): Express these in bytes instead of percentages to avoid
  // rounding errors?
  config->arena_percentages[kArenaType_BufferPool] = 0.85f;
  config->arena_percentages[kArenaType_MetaData] = 0.04f;
  config->arena_percentages[kArenaType_TransferWindow] = 0.08f;
  config->arena_percentages[kArenaType_Transient] = 0.03f;

  config->mount_points[0] = mem_mount_point;
  config->mount_points[1] = nvme_mount_point;
  config->mount_points[2] = bb_mount_point;
  config->mount_points[3] = pfs_mount_point;

  config->rpc_server_base_name = "localhost";
  config->rpc_server_suffix = "";
  config->rpc_protocol = "ofi+sockets";
  config->rpc_domain = "";
  config->rpc_port = 8080;
  config->rpc_num_threads = 1;

  config->max_buckets_per_node = 16;
  config->max_vbuckets_per_node = 8;
  config->system_view_state_update_interval_ms = 100;

  size_t shmem_name_size = strlen(buffer_pool_shmem_name);
  for (size_t i = 0; i < shmem_name_size; ++i) {
    config->buffer_pool_shmem_name[i] = buffer_pool_shmem_name[i];
  }
  config->buffer_pool_shmem_name[shmem_name_size] = '\0';
}

ArenaInfo GetArenaInfo(Config *config) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  // NOTE(chogan): Assumes first Tier is RAM
  size_t total_hermes_memory = RoundDownToMultiple(config->capacities[0],
                                                   page_size);
  size_t total_pages = total_hermes_memory / page_size;
  size_t pages_left = total_pages;

  ArenaInfo result = {};

  for (int i = 0; i < kArenaType_Count; ++i) {
    size_t pages {0};
    if (i < kArenaType_Count-1) {
      pages = std::floor(config->arena_percentages[i] * total_pages);
      pages_left -= pages;
    }
    else {
      pages = pages_left;
      pages_left = 0;
    }
    size_t num_bytes = pages * page_size;
    result.sizes[i] = num_bytes;
    result.total += num_bytes;
  }

  assert(pages_left == 0);

  return result;
}

// TODO(chogan): Move into library
SharedMemoryContext InitHermesCore(Config *config, CommunicationContext *comm,
                                   ArenaInfo *arena_info, Arena *arenas,
                                   RpcContext *rpc) {

  size_t shmem_size = (arena_info->total -
                       arena_info->sizes[kArenaType_Transient]);
  u8 *shmem_base = InitSharedMemory(config->buffer_pool_shmem_name, shmem_size);

  // NOTE(chogan): Initialize shared arenas
  ptrdiff_t base_offset = 0;
  for (int i = 0; i < kArenaType_Count; ++i) {
    if (i == kArenaType_Transient) {
      // NOTE(chogan): Transient arena exists per rank, not in shared memory
      continue;
    }
    size_t arena_size = arena_info->sizes[i];
    InitArena(&arenas[i], arena_size, shmem_base + base_offset);
    base_offset += arena_size;
  }

  SharedMemoryContext context = {};
  context.shm_base = shmem_base;
  context.shm_size = shmem_size;
  context.buffer_pool_offset = InitBufferPool(context.shm_base,
                                              &arenas[kArenaType_BufferPool],
                                              &arenas[kArenaType_Transient],
                                              comm->node_id, config);

  MetadataManager *mdm =
    PushClearedStruct<MetadataManager>(&arenas[kArenaType_MetaData]);
  context.metadata_manager_offset = (u8 *)mdm - (u8 *)shmem_base;

  rpc->state = CreateRpcState(&arenas[kArenaType_MetaData]);
  mdm->rpc_state_offset = (u8 *)rpc->state - shmem_base;

  InitMetadataManager(mdm, &arenas[kArenaType_MetaData], config, comm->node_id);

  // NOTE(chogan): Store the metadata_manager_offset right after the
  // buffer_pool_offset so other processes can pick it up.
  ptrdiff_t *metadata_manager_offset_location =
    (ptrdiff_t *)(shmem_base + sizeof(context.buffer_pool_offset));
  *metadata_manager_offset_location = context.metadata_manager_offset;

  return context;
}

SharedMemoryContext
BootstrapSharedMemory(Arena *arenas, Config *config, CommunicationContext *comm,
                      RpcContext *rpc, bool is_daemon, bool is_adapter) {
  size_t bootstrap_size = KILOBYTES(4);
  u8 *bootstrap_memory = (u8 *)malloc(bootstrap_size);
  InitArena(&arenas[kArenaType_Transient], bootstrap_size, bootstrap_memory);

  ArenaInfo arena_info = GetArenaInfo(config);
  // NOTE(chogan): The buffering capacity for the RAM Tier is the size of the
  // BufferPool Arena
  config->capacities[0] = arena_info.sizes[kArenaType_BufferPool];

  size_t trans_arena_size =
    InitCommunication(comm, &arenas[kArenaType_Transient],
                      arena_info.sizes[kArenaType_Transient], is_daemon,
                      is_adapter);

  GrowArena(&arenas[kArenaType_Transient], trans_arena_size);
  comm->state = arenas[kArenaType_Transient].base;

  InitRpcContext(rpc, comm->num_nodes, comm->node_id, config);

  SharedMemoryContext result = {};
  if (comm->proc_kind == ProcessKind::kHermes && comm->first_on_node) {
    result = InitHermesCore(config, comm, &arena_info, arenas, rpc);
  }

  return result;
}

// TODO(chogan): Move into library
std::shared_ptr<api::Hermes> InitHermes(const char *config_file=NULL,
                                        bool is_daemon=false,
                                        bool is_adapter=false) {
  hermes::Config config = {};
  const size_t config_memory_size = KILOBYTES(16);
  hermes::u8 config_memory[config_memory_size];

  if (config_file) {
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, config_memory_size, config_memory);
    hermes::ParseConfig(&config_arena, config_file, &config);
  } else {
    InitTestConfig(&config);
  }
  std::string base_shmem_name(config.buffer_pool_shmem_name);
  MakeFullShmemName(config.buffer_pool_shmem_name, base_shmem_name.c_str());

  // TODO(chogan): Do we need a transfer window arena? We can probably just use
  // the transient arena for this.
  Arena arenas[kArenaType_Count] = {};
  CommunicationContext comm = {};
  RpcContext rpc = {};
  SharedMemoryContext context =
    BootstrapSharedMemory(arenas, &config, &comm, &rpc, is_daemon, is_adapter);

  WorldBarrier(&comm);
  std::shared_ptr<api::Hermes> result = nullptr;

  if (comm.proc_kind == ProcessKind::kHermes) {
    result = std::make_shared<api::Hermes>(context);
    result->shmem_name_ = std::string(config.buffer_pool_shmem_name);
  } else {
    context = GetSharedMemoryContext(config.buffer_pool_shmem_name);
    SubBarrier(&comm);
    result = std::make_shared<api::Hermes>(context);
    // NOTE(chogan): Give every App process a valid pointer to the internal RPC
    // state in shared memory
    MetadataManager *mdm = GetMetadataManagerFromContext(&context);
    rpc.state = (void *)(context.shm_base + mdm->rpc_state_offset);
  }
  bool create_shared_files = (comm.proc_kind == ProcessKind::kHermes &&
                              comm.first_on_node);
  InitFilesForBuffering(&context, create_shared_files);

  WorldBarrier(&comm);

  rpc.node_id = comm.node_id;
  rpc.num_nodes = comm.num_nodes;

  result->trans_arena_ = arenas[kArenaType_Transient];
  result->comm_ = comm;
  result->context_ = context;
  result->rpc_ = rpc;

  // NOTE(chogan): The RPC servers have to be started here because they need to
  // save a reference to the context and rpc instances that are members of the
  // Hermes instance.
  if (comm.proc_kind == ProcessKind::kHermes) {
    std::string host_number = GetHostNumberAsString(&result->rpc_,
                                                    result->rpc_.node_id);

    std::string rpc_server_addr = config.rpc_protocol + "://";

    if (!config.rpc_domain.empty()) {
      rpc_server_addr += config.rpc_domain + "/";
    }
    rpc_server_addr += (config.rpc_server_base_name + host_number +
                        config.rpc_server_suffix + ":" +
                        std::to_string(config.rpc_port));

    result->rpc_.start_server(&result->context_, &result->rpc_,
                              &result->trans_arena_, rpc_server_addr.c_str(),
                              config.rpc_num_threads);
    double sleep_ms = config.system_view_state_update_interval_ms;
    StartGlobalSystemViewStateUpdateThread(&result->context_, &result->rpc_,
                                           &result->trans_arena_,
                                           sleep_ms);
  }

  WorldBarrier(&comm);

  return result;
}

// TODO(chogan): Move into library
std::shared_ptr<api::Hermes> InitHermesClient(const char *config_file=NULL) {
  std::shared_ptr<api::Hermes> result = InitHermes(config_file, false, true);

  return result;
}

// TODO(chogan): Move into library
std::shared_ptr<api::Hermes> InitHermesDaemon(char *config_file=NULL) {
  std::shared_ptr<api::Hermes> result = InitHermes(config_file, true);

  return result;
}

}  // namespace hermes
#endif  // HERMES_COMMON_H_
