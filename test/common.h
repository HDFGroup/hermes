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

/**
 * @file common.h
 *
 * Common configuration and utilities for tests.
 */

namespace hermes {

#define KILOBYTES(n) (1024 * (n))
#define MEGABYTES(n) (1024 * 1024 * (n))

const char mem_mount_point[] = "";
const char nvme_mount_point[] = "/home/user/nvme/";
const char bb_mount_point[] = "/mount/burst_buffer/";
const char pfs_mount_point[] = "/mount/pfs/";
const char buffer_pool_shmem_name[] = "/hermes_buffer_pool_";
const char rpc_server_name[] = "tcp://172.20.101.25:8080";

void InitTestConfig(Config *config) {
  // TODO(chogan): @configuration This will come from Apollo or a config file
  config->num_tiers = 4;
  assert(config->num_tiers < kMaxTiers);

  for (int tier = 0; tier < config->num_tiers; ++tier) {
    config->capacities[tier] = MEGABYTES(50);
    config->block_sizes[tier] = KILOBYTES(4);
    config->num_slabs[tier] = 4;

    config->slab_unit_sizes[tier][0] = 1;
    config->slab_unit_sizes[tier][1]= 4;
    config->slab_unit_sizes[tier][2]= 16;
    config->slab_unit_sizes[tier][3]= 32;

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
  config->arena_percentages[kArenaType_BufferPool] = 0.85f;
  config->arena_percentages[kArenaType_MetaData] = 0.04f;
  config->arena_percentages[kArenaType_TransferWindow] = 0.08f;
  config->arena_percentages[kArenaType_Transient] = 0.03f;
  config->mount_points[0] = mem_mount_point;
  config->mount_points[1] = nvme_mount_point;
  config->mount_points[2] = bb_mount_point;
  config->mount_points[3] = pfs_mount_point;
  config->rpc_server_name = rpc_server_name;

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

// TODO(chogan): Move into library
SharedMemoryContext InitHermesCore(Config *config, CommunicationContext *comm,
                                   bool start_rpc_server,
                                   int num_rpc_threads=0) {

  ArenaInfo arena_info = GetArenaInfo(config);
  SharedMemoryContext context = {};

  int shmem_fd = shm_open(config->buffer_pool_shmem_name, O_CREAT | O_RDWR,
                          S_IRUSR | S_IWUSR);
  if (shmem_fd >= 0) {
    ftruncate(shmem_fd, arena_info.total);
    // TODO(chogan): Should we mlock() the segment to prevent page faults?
    u8 *hermes_memory = (u8 *)mmap(0, arena_info.total, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, shmem_fd, 0);
    // TODO(chogan): @errorhandling
    close(shmem_fd);

    if (hermes_memory) {
      // TODO(chogan): At the moment, memory management as a whole is a bit
      // conflated with memory management for the BufferPool. I think eventually
      // we will only need the buffer_pool_arena here and the other three arenas
      // can be moved.

      // TODO(chogan): Store arenas that need stored
      Arena arenas[kArenaType_Count] = {};
      ptrdiff_t base_offset = 0;
      for (int i = 0; i < kArenaType_Count; ++i) {
        size_t arena_size = arena_info.sizes[i];
        InitArena(&arenas[i], arena_size, hermes_memory + base_offset);
        base_offset += arena_size;
      }

      // NOTE(chogan): We my have adjusted the RAM capacity for page size or
      // alignment, so update the config
      config->capacities[0] = arena_info.sizes[kArenaType_BufferPool];
      context.shm_base = hermes_memory;
      context.shm_size = arena_info.total;

      context.buffer_pool_offset =
        InitBufferPool(hermes_memory, &arenas[kArenaType_BufferPool],
                       &arenas[kArenaType_Transient], comm->node_id, config);

      WorldBarrier(comm);

      if (start_rpc_server) {
        StartBufferPoolRpcServer(&context, config->rpc_server_name,
                                 num_rpc_threads);
      }
    } else {
      perror("Couldn't map shared memory");
    }
  } else {
    perror("Couldn't create shared memory");
  }

  return context;
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
  config.mount_points[0] = "";
  config.mount_points[1] = "./";
  config.mount_points[2] = "./";
  config.mount_points[3] = "./";

  Arena arenas[kArenaType_Count] = {};
  size_t bootstrap_size = KILOBYTES(4);
  u8 *bootstrap_memory = (u8 *)malloc(bootstrap_size);
  InitArena(&arenas[kArenaType_Transient], bootstrap_size, bootstrap_memory);

  ArenaInfo arena_info = GetArenaInfo(&config);
  // InitSharedMemory(config);

  CommunicationContext comm = {};
  size_t trans_arena_size =
    InitCommunication(&comm, &arenas[kArenaType_Transient],
                      arena_info.sizes[kArenaType_Transient], false);

  GrowArena(&arenas[kArenaType_Transient], trans_arena_size);

  // TEMP(chogan):
  // free(bootstrap_memory);

  std::shared_ptr<api::Hermes> result = nullptr;
  SharedMemoryContext context = {};

  if (comm.proc_kind == ProcessKind::kHermes) {
    context = InitHermesCore(&config, &comm, false);
    WorldBarrier(&comm);
    result = std::make_shared<api::Hermes>(context);
    result->shmem_name_ = std::string(config.buffer_pool_shmem_name);
  } else {
    WorldBarrier(&comm);

    context = hermes::InitHermesClient(&comm, config.buffer_pool_shmem_name,
                                       true);
    result = std::make_shared<api::Hermes>(context);
  }

  result->comm_ = comm;

  return result;
}

}  // namespace hermes
#endif  // HERMES_COMMON_H_
