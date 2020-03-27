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
// TEMP(chogan): std::cin.get()
#include <iostream>

#include "buffer_pool.h"
#include "buffer_pool_internal.h"

namespace hermes {

#define KILOBYTES(n) (1024 * (n))
#define MEGABYTES(n) (1024 * 1024 * (n))

const char mem_mount_point[] = "";
const char nvme_mount_point[] = "/home/user/nvme/";
const char bb_mount_point[] = "/mount/burst_buffer/";
const char pfs_mount_point[] = "/mount/pfs/";
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
  config->buffer_pool_memory_percent = 0.85f;
  config->metadata_memory_percent = 0.04f;
  config->transfer_window_memory_percent = 0.08f;
  config->transient_memory_percent = 0.03f;
  config->mount_points[0] = mem_mount_point;
  config->mount_points[1] = nvme_mount_point;
  config->mount_points[2] = bb_mount_point;
  config->mount_points[3] = pfs_mount_point;
  config->rpc_server_name = rpc_server_name;

  MakeFullShmemName(config->buffer_pool_shmem_name, buffer_pool_shmem_name);
}

SharedMemoryContext InitHermesCore(Config *config, bool start_rpc_server,
                                   int num_rpc_threads=0, bool init_mpi=false) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  // NOTE(chogan): Assumes first Tier is RAM
  size_t total_hermes_memory = RoundDownToMultiple(config->capacities[0],
                                                   page_size);
  size_t total_pages = total_hermes_memory / page_size;

  size_t buffer_pool_pages = std::floor(config->buffer_pool_memory_percent *
                                        total_pages);
  size_t pages_left = total_pages - buffer_pool_pages;

  size_t metadata_memory_pages = std::floor(config->metadata_memory_percent *
                                            total_pages);
  pages_left -= metadata_memory_pages;

  size_t transfer_window_memory_pages =
    std::floor(config->transfer_window_memory_percent * total_pages);
  pages_left -= transfer_window_memory_pages;

  size_t transient_memory_pages = std::floor(config->transient_memory_percent *
                                             total_pages);
  pages_left -= transient_memory_pages;

  size_t buffer_pool_memory_size = buffer_pool_pages * page_size;
  size_t metadata_memory_size = metadata_memory_pages * page_size;
  size_t transfer_window_memory_size = transfer_window_memory_pages * page_size;
  size_t transient_memory_size = transient_memory_pages * page_size;

  SharedMemoryContext context = {};

  int shmem_fd = shm_open(config->buffer_pool_shmem_name, O_CREAT | O_RDWR,
                          S_IRUSR | S_IWUSR);
  if (shmem_fd >= 0) {
    ftruncate(shmem_fd, total_hermes_memory);
    // TODO(chogan): Should we mlock() the segment to prevent page faults?
    u8 *hermes_memory = (u8 *)mmap(0, total_hermes_memory,
                                   PROT_READ | PROT_WRITE, MAP_SHARED, shmem_fd,
                                   0);
    // TODO(chogan): @errorhandling
    close(shmem_fd);

    if (hermes_memory) {
      // TODO(chogan): At the moment, memory management as a whole is a bit
      // conflated with memory management for the BufferPool. I think eventually
      // we will only need the buffer_pool_arena here and the other three arenas
      // can be moved.
      Arena buffer_pool_arena = {};
      InitArena(&buffer_pool_arena, buffer_pool_memory_size, hermes_memory);

      Arena metadata_arena = {};
      InitArena(&metadata_arena, metadata_memory_size,
                hermes_memory + buffer_pool_memory_size);

      Arena transfer_window_arena = {};
      InitArena(&transfer_window_arena, transfer_window_memory_size,
                hermes_memory + buffer_pool_memory_size + metadata_memory_size);

      Arena transient_arena = {};
      InitArena(&transient_arena, transient_memory_size,
                (hermes_memory + buffer_pool_memory_size +
                 metadata_memory_size + transfer_window_memory_size));

      // TODO(chogan): Maybe we need a persistent_storage_arena?
      InitCommunication(&metadata_arena, &context, init_mpi);

      context.comm_api.get_node_info(&context.comm_state, &transient_arena);
      assert(context.comm_state.node_id > 0);
      assert(context.comm_state.num_nodes > 0);

      // NOTE(chogan): We my have changed the RAM capacity for page size or
      // alignment, so update the config->
      config->capacities[0] = buffer_pool_memory_size;
      context.shm_base = hermes_memory;
      context.shm_size = total_hermes_memory;

      context.buffer_pool_offset = InitBufferPool(hermes_memory,
                                                  &buffer_pool_arena,
                                                  &transient_arena,
                                                  context.comm_state.node_id,
                                                  config);

      context.comm_api.world_barrier(&context.comm_state);

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

SharedMemoryContext InitHermesClient(int rank, bool init_buffering_files) {
  char full_shmem_name[kMaxBufferPoolShmemNameLength];
  char base_shmem_name[] = "/hermes_buffer_pool_";
  MakeFullShmemName(full_shmem_name, base_shmem_name);
  SharedMemoryContext context = GetSharedMemoryContext(full_shmem_name);
  // TODO(chogan): Proper application core comms intialization
  context.comm_state.app_proc_id = rank;

  if (init_buffering_files) {
    InitFilesForBuffering(&context); 
  }

  return context;
}

}  // namespace hermes
#endif  // HERMES_COMMON_H_
