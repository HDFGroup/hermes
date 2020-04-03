#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hermes_types.h"
#include "memory_arena.h"

// TEMP(chogan): Find a home for this declaration
namespace hermes {
void ParseConfig(Arena *arena, const char *path, Config *config);
}

int main(int argc, char **argv) {

  if (argc < 2) {
    fprintf(stderr, "Please pass the path to a hermes.conf file\n");
    return -1;
  }

  hermes::Config config = {};

  size_t config_memory_size = 8 * 1024;
  hermes::u8 *config_memory = (hermes::u8 *)malloc(config_memory_size);
  hermes::Arena arena = {};
  hermes::InitArena(&arena, config_memory_size, config_memory);

  hermes::ParseConfig(&arena, argv[1], &config);

  // TODO(chogan): Use glog's CHECK so this test runs in release mode?
  assert(config.num_tiers == 4);
  for (int i = 0; i < config.num_tiers; ++i) {
    assert(config.capacities[i] == 50);
    assert(config.block_sizes[i] == 4);
    assert(config.num_slabs[i] == 4);

    assert(config.slab_unit_sizes[i][0] == 1);
    assert(config.slab_unit_sizes[i][1] == 4);
    assert(config.slab_unit_sizes[i][2] == 16);
    assert(config.slab_unit_sizes[i][3] == 32);

    for (int j = 0; j < config.num_slabs[i]; ++j) {
      assert(config.desired_slab_percentages[i][j] == 0.25f);
    }
  }

  assert(config.bandwidths[0] == 6000.0f);
  assert(config.bandwidths[1] == 300.0f);
  assert(config.bandwidths[2] == 150.0f);
  assert(config.bandwidths[3] == 70.0f);

  assert(config.latencies[0] == 15.0f);
  assert(config.latencies[1] == 250000.0f);
  assert(config.latencies[2] == 500000.0f);
  assert(config.latencies[3] == 1000000.0f);

  assert(config.arena_percentages[hermes::kArenaType_BufferPool] == 0.85f);
  assert(config.arena_percentages[hermes::kArenaType_MetaData] == 0.04f);
  assert(config.arena_percentages[hermes::kArenaType_Transient] == 0.03f);
  assert(config.arena_percentages[hermes::kArenaType_TransferWindow] == 0.08f);

  assert(strncmp(config.mount_points[0], "", 0) == 0);
  assert(strncmp(config.mount_points[1], "./", 2) == 0);
  assert(strncmp(config.mount_points[2], "./", 2) == 0);
  assert(strncmp(config.mount_points[3], "./", 2) == 0);
  const char expected_name[] = "sockets://localhost:8080";
  assert(strncmp(config.rpc_server_name, expected_name,
                 sizeof(expected_name)) == 0);

  // TEMP(chogan):
  for (int i = 0; i < config.num_tiers; ++i) {
    free((void *)config.mount_points[i]);
  }
  free((void *)config.rpc_server_name);

  hermes::DestroyArena(&arena);

  return 0;
}
