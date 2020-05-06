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
  // TODO(chogan): @configuration This will come from Apollo or a config file
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
  config->rpc_server_name = rpc_server_name;

  config->max_buckets_per_node = 16;
  config->max_vbuckets_per_node = 8;

  MakeFullShmemName(config->buffer_pool_shmem_name, buffer_pool_shmem_name);
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
    size_t pages = std::floor(config->arena_percentages[i] * total_pages);
    pages_left -= pages;
    size_t num_bytes = pages * page_size;
    result.sizes[i] = num_bytes;
    result.total += num_bytes;
  }

  assert(pages_left == 0);

  return result;
}

u8 *InitSharedMemory(const char *shmem_name, size_t total_size) {
  u8 *result = 0;
  int shmem_fd = shm_open(shmem_name, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

  if (shmem_fd >= 0) {
    ftruncate(shmem_fd, total_size);
    // TODO(chogan): Should we mlock() the segment to prevent page faults?
    result = (u8 *)mmap(0, total_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        shmem_fd, 0);
    // TODO(chogan): @errorhandling
    close(shmem_fd);
  } else {
    // TODO(chogan): @errorhandling
    assert(!"shm_open failed\n");
  }

  // TODO(chogan): @errorhandling
  assert(result);

  return result;
}

// TODO(chogan): Move into library
SharedMemoryContext InitHermesCore(Config *config, CommunicationContext *comm,
                                   ArenaInfo *arena_info, Arena *arenas,
                                   bool start_rpc_server, RpcContext *rpc,
                                   int num_rpc_threads=0) {

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

  // NOTE(chogan): Move comm->state from transient arena to metadata arena.
  // Assumes that comm->state is the only thing in the transient arena
  size_t comm_state_size = arenas[kArenaType_Transient].used;
  void *comm_state_dest = PushSize(&arenas[kArenaType_MetaData],
                                   comm_state_size);
  comm->copy_state(comm->state, comm_state_dest);
  comm->state = comm_state_dest;

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
  InitMetadataManager(mdm, &arenas[kArenaType_MetaData], config, comm->node_id);

  // NOTE(chogan): Store the metadata_manager_offset right after the
  // buffer_pool_offset so other processes can pick it up.
  ptrdiff_t *metadata_manager_offset_location =
    (ptrdiff_t *)(shmem_base + sizeof(context.buffer_pool_offset));
  *metadata_manager_offset_location = context.metadata_manager_offset;

  // NOTE(chogan): Store the offset from the MDM to the communication state too.
  ptrdiff_t *comm_state_offset_location =
    (ptrdiff_t *)((u8 *)metadata_manager_offset_location +
                  sizeof(context.metadata_manager_offset));
  ptrdiff_t comm_state_offset_from_mdm = (u8 *)mdm - (u8 *)comm_state_dest;
  *comm_state_offset_location = comm_state_offset_from_mdm;

  if (start_rpc_server) {
    rpc->start_server(&context, rpc,
                      config->rpc_server_name.c_str(), num_rpc_threads);
  }

  return context;
}

SharedMemoryContext
BootstrapSharedMemory(Arena *arenas, Config *config, CommunicationContext *comm,
                      RpcContext *rpc, int num_rpc_threads, bool is_daemon) {
  size_t bootstrap_size = KILOBYTES(4);
  u8 *bootstrap_memory = (u8 *)malloc(bootstrap_size);
  InitArena(&arenas[kArenaType_Transient], bootstrap_size, bootstrap_memory);

  ArenaInfo arena_info = GetArenaInfo(config);
  // NOTE(chogan): The buffering capacity for the RAM Tier is the size of the
  // BufferPool Arena
  config->capacities[0] = arena_info.sizes[kArenaType_BufferPool];

  size_t trans_arena_size =
    InitCommunication(comm, &arenas[kArenaType_Transient],
                      arena_info.sizes[kArenaType_Transient], is_daemon);

  GrowArena(&arenas[kArenaType_Transient], trans_arena_size);
  comm->state = arenas[kArenaType_Transient].base;

  // TODO(chogan): Make sure each process has an udpated RpcContext
  InitRpcContext(rpc, comm->num_nodes, comm->node_id);

  SharedMemoryContext result = {};
  // TODO(chogan): Always start RPC server (remove is_daemon param)
  if (comm->proc_kind == ProcessKind::kHermes && comm->first_on_node) {
    result = InitHermesCore(config, comm, &arena_info, arenas, is_daemon,
                            rpc, num_rpc_threads);
  }

  return result;
}

// TODO(chogan): Move into library
std::shared_ptr<api::Hermes>
InitDaemon(const std::string &buffering_path,
           const std::string &rpc_server_name, int num_rpc_threads) {
  // TODO(chogan): Read Config from a file
  hermes::Config config = {};
  InitTestConfig(&config);

  config.rpc_server_name = rpc_server_name.c_str();

  if (buffering_path.size() > 0) {
    for (int i = 1; i < config.num_tiers; ++i) {
      config.mount_points[i] = buffering_path.c_str();
    }
  }

  Arena arenas[kArenaType_Count] = {};
  CommunicationContext comm = {};
  RpcContext rpc = {};
  SharedMemoryContext context =
    BootstrapSharedMemory(arenas, &config, &comm, &rpc, num_rpc_threads, true);

  std::shared_ptr<api::Hermes> result = std::make_shared<api::Hermes>(context);
  result->shmem_name_ = std::string(config.buffer_pool_shmem_name);
  result->trans_arena_ = arenas[kArenaType_Transient];
  // NOTE(chogan): Reset the transient arena since we're done with the data it
  // contains
  result->trans_arena_.used = 0;
  result->comm_ = comm;
  result->context_ = context;
  result->rpc_ = rpc;

  return result;
}

// TODO(chogan): Move into library
SharedMemoryContext InitHermesClient(CommunicationContext *comm,
                                     char *shmem_name,
                                     bool init_buffering_files) {
  SharedMemoryContext context = GetSharedMemoryContext(shmem_name);

  if (init_buffering_files) {
    InitFilesForBuffering(&context, comm->app_proc_id == 0);
  }

  return context;
}

// TODO(chogan): Move into library
std::shared_ptr<api::Hermes> InitHermes() {
  // TODO(chogan): Read Config from a file
  hermes::Config config = {};
  InitTestConfig(&config);

  // TODO(chogan): The metadata arena will be a different type of arena because
  // it needs to be thread-safe, as it's shared among all ranks. For now we
  // create it, put the CommnciationContext::state in it, and then ignore it.
  // We'll probably put an HCL container on top of it. The transfer window arena
  // will probably become local to each rank.
  Arena arenas[kArenaType_Count] = {};
  CommunicationContext comm = {};
  RpcContext rpc = {};
  // TODO(chogan): start RPC server by default, pass num_rpc_threads
  SharedMemoryContext context =
    BootstrapSharedMemory(arenas, &config, &comm, &rpc, 0, false);

  WorldBarrier(&comm);
  std::shared_ptr<api::Hermes> result = nullptr;

  if (comm.proc_kind == ProcessKind::kHermes) {
    result = std::make_shared<api::Hermes>(context);
    result->shmem_name_ = std::string(config.buffer_pool_shmem_name);
  } else {
    context = GetSharedMemoryContext(config.buffer_pool_shmem_name);

    if (comm.first_on_node) {
      MetadataManager *mdm = GetMetadataManagerFromContext(&context);
      ptrdiff_t *comm_state_offset_location =
        (ptrdiff_t *)(context.shm_base + sizeof(context.buffer_pool_offset) +
                      sizeof(context.metadata_manager_offset));
      void *comm_state_location = (u8 *)mdm - *comm_state_offset_location;
      comm.sync_comm_state(comm_state_location, comm.state);
      comm.state = comm_state_location;
    }

    AppBarrier(&comm);

    InitFilesForBuffering(&context, comm.app_proc_id == 0);
    result = std::make_shared<api::Hermes>(context);
  }

  WorldBarrier(&comm);

  result->trans_arena_ = arenas[kArenaType_Transient];
  // NOTE(chogan): Reset the transient arena since we're done with the data it
  // contains
  result->trans_arena_.used = 0;
  result->comm_ = comm;
  result->context_ = context;
  result->rpc_ = rpc;

  return result;
}

}  // namespace hermes
#endif  // HERMES_COMMON_H_
