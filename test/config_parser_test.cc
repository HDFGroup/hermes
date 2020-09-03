#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hermes_types.h"
#include "memory_management.h"
#include "buffer_pool_internal.h"
#include "test_utils.h"

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "Expected a path to a hermes.conf file\n");
    exit(-1);
  }

  hermes::Config config = {};

  const size_t kConfigMemorySize = KILOBYTES(7);
  hermes::u8 config_memory[kConfigMemorySize];
  hermes::Arena arena = {};
  hermes::InitArena(&arena, kConfigMemorySize, config_memory);

  hermes::ParseConfig(&arena, argv[1], &config);

  Assert(config.num_devices == 4);
  Assert(config.num_targets == 4);
  for (int i = 0; i < config.num_devices; ++i) {
    Assert(config.capacities[i] == MEGABYTES(50));
    Assert(config.block_sizes[i] == KILOBYTES(4));
    Assert(config.num_slabs[i] == 4);

    Assert(config.slab_unit_sizes[i][0] == 1);
    Assert(config.slab_unit_sizes[i][1] == 4);
    Assert(config.slab_unit_sizes[i][2] == 16);
    Assert(config.slab_unit_sizes[i][3] == 32);

    for (int j = 0; j < config.num_slabs[i]; ++j) {
      Assert(config.desired_slab_percentages[i][j] == 0.25f);
    }
  }

  Assert(config.bandwidths[0] == 6000.0f);
  Assert(config.bandwidths[1] == 300.0f);
  Assert(config.bandwidths[2] == 150.0f);
  Assert(config.bandwidths[3] == 70.0f);

  Assert(config.latencies[0] == 15.0f);
  Assert(config.latencies[1] == 250000.0f);
  Assert(config.latencies[2] == 500000.0f);
  Assert(config.latencies[3] == 1000000.0f);

  Assert(config.arena_percentages[hermes::kArenaType_BufferPool] == 0.85f);
  Assert(config.arena_percentages[hermes::kArenaType_MetaData] == 0.04f);
  Assert(config.arena_percentages[hermes::kArenaType_Transient] == 0.03f);
  Assert(config.arena_percentages[hermes::kArenaType_TransferWindow] == 0.08f);

  Assert(config.mount_points[0] == "");
  Assert(config.mount_points[1] == "./");
  Assert(config.mount_points[2] == "./");
  Assert(config.mount_points[3] == "./");

  Assert(config.max_buckets_per_node == 16);
  Assert(config.max_vbuckets_per_node == 8);
  Assert(config.system_view_state_update_interval_ms == 1000);

  Assert(config.rpc_protocol == "ofi+sockets");
  Assert(config.rpc_domain.empty());
  Assert(config.rpc_port == 8080);
  Assert(config.buffer_organizer_port == 8081);
  Assert(config.rpc_host_number_range[0] == 0 &&
         config.rpc_host_number_range[1] == 0);
  Assert(config.rpc_num_threads == 1);

  const char expected_rpc_server_name[] = "localhost";
  Assert(config.rpc_server_base_name == expected_rpc_server_name);
  Assert(config.rpc_server_suffix.empty());

  const char expected_shm_name[] = "/hermes_buffer_pool_";
  Assert(strncmp(config.buffer_pool_shmem_name, expected_shm_name,
                 sizeof(expected_shm_name)) == 0);

  return 0;
}
