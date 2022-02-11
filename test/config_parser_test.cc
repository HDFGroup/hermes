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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hermes_types.h"
#include "memory_management.h"
#include "buffer_pool_internal.h"
#include "test_utils.h"
#include "config_parser.h"


using hermes::Arena;
using hermes::u8;
using hermes::Config;

namespace hermes {
namespace testing {

Config ParseConfigString(Arena *arena, const std::string &config_string) {
  hermes::ScopedTemporaryMemory scratch(arena);
  hermes::EntireFile config_file =
    {(u8 *)config_string.data(), config_string.size()};
  hermes::TokenList tokens = hermes::Tokenize(scratch, config_file);
  Config config = {};
  InitDefaultConfig(&config);
  hermes::ParseTokens(&tokens, &config);

  return config;
}

void RunHostNumbersTest(Arena *arena, const std::string &config_string,
                        const std::vector<int> &expected) {
  Config config = ParseConfigString(arena, config_string);
  Assert(config.host_numbers == expected);
}

void TestParseRangeList(Arena *arena) {
  {
    std::vector<int> expected{1, 3, 4, 5, 7, 10, 11, 12, 13, 14};
    RunHostNumbersTest(arena, "rpc_host_number_range = {1, 3-5, 7, 10-14};\n",
                       expected);
  }

  {
    std::vector<int> expected{1};
    RunHostNumbersTest(arena, "rpc_host_number_range = {1};\n", expected);
  }

  {
    std::vector<int> expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    RunHostNumbersTest(arena, "rpc_host_number_range = {1-10};\n", expected);
  }

  {
    std::vector<int> expected{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12};
    RunHostNumbersTest(arena, "rpc_host_number_range = {1-10, 12};\n",
                       expected);
  }

  {
    std::vector<int> expected;
    RunHostNumbersTest(arena, "rpc_host_number_range = {};\n", expected);
  }
}

void RunCapacityValuesTest(Arena *arena, const std::string &config_string,
                           const std::vector<size_t> &expected) {
  Config config = ParseConfigString(arena, config_string);
  Assert((size_t)config.num_devices == expected.size());
  for (int i = 0; i < config.num_devices; ++i) {
    Assert(config.capacities[i] == expected[i]);
  }
}

void TestCapacityValues(Arena *arena) {
  std::string base_config = "num_devices = 4;\n";

  {
    std::vector<size_t> expected{50, 50, 50, 50};
    std::string config_string = "capacities_bytes = {50, 50, 50, 50};\n";
    RunCapacityValuesTest(arena, base_config + config_string, expected);
  }

  {
    std::vector<size_t> expected{KILOBYTES(50), KILOBYTES(50), KILOBYTES(50),
                                 KILOBYTES(50)};
    std::string config_string = "capacities_kb = {50, 50, 50, 50};\n";
    RunCapacityValuesTest(arena, base_config + config_string, expected);
  }

  {
    std::vector<size_t> expected{MEGABYTES(50), MEGABYTES(50), MEGABYTES(50),
                                 MEGABYTES(50)};
    std::string config_string = "capacities_mb = {50, 50, 50, 50};\n";
    RunCapacityValuesTest(arena, base_config + config_string, expected);
  }

  {
    std::vector<size_t> expected{GIGABYTES(50), GIGABYTES(50), GIGABYTES(50),
                                 GIGABYTES(50)};
    std::string config_string = "capacities_gb = {50, 50, 50, 50};\n";
    RunCapacityValuesTest(arena, base_config + config_string, expected);
  }
}

void RunBlockSizesTest(Arena *arena, const std::string &config_string,
                       const std::vector<int> &expected) {
  Config config = ParseConfigString(arena, config_string);
  Assert((size_t)config.num_devices == expected.size());
  for (int i = 0; i < config.num_devices; ++i) {
    Assert(config.block_sizes[i] == expected[i]);
  }
}

void TestBlockSizes(Arena *arena) {
  std::string base_config = "num_devices = 4;\n";

  {
    std::vector<int> expected{50, 50, 50, 50};
    std::string config_string = "block_sizes_bytes = {50, 50, 50, 50};\n";
    RunBlockSizesTest(arena, base_config + config_string, expected);
  }
  {
    std::vector<int> expected{KILOBYTES(50), KILOBYTES(50), KILOBYTES(50),
                              KILOBYTES(50)};
    std::string config_string = "block_sizes_kb = {50, 50, 50, 50};\n";
    RunBlockSizesTest(arena, base_config + config_string, expected);
  }
  {
    std::vector<int> expected{MEGABYTES(50), MEGABYTES(50), MEGABYTES(50),
                              MEGABYTES(50)};
    std::string config_string = "block_sizes_mb = {50, 50, 50, 50};\n";
    RunBlockSizesTest(arena, base_config + config_string, expected);
  }
  {
    std::vector<int> expected{GIGABYTES(1), GIGABYTES(1), GIGABYTES(1),
                              GIGABYTES(1)};
    std::string config_string =  "block_sizes_gb = {1, 1, 1, 1};\n";
    RunBlockSizesTest(arena, base_config + config_string, expected);
  }
}

void TestDefaultConfig(Arena *arena, const char *config_file) {
  hermes::Config config = {};
  hermes::ParseConfig(arena, config_file, &config);

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
  Assert(config.arena_percentages[hermes::kArenaType_Transient] == 0.11f);

  Assert(config.mount_points[0] == "");
  Assert(config.mount_points[1] == "./");
  Assert(config.mount_points[2] == "./");
  Assert(config.mount_points[3] == "./");
  Assert(config.swap_mount == "./");
  Assert(config.num_buffer_organizer_retries == 3);

  Assert(config.max_buckets_per_node == 16);
  Assert(config.max_vbuckets_per_node == 8);
  Assert(config.system_view_state_update_interval_ms == 1000);

  Assert(config.rpc_protocol == "ofi+sockets");
  Assert(config.rpc_domain.empty());
  Assert(config.rpc_port == 8080);
  Assert(config.buffer_organizer_port == 8081);
  Assert(config.rpc_num_threads == 1);

  const char expected_rpc_server_name[] = "localhost";
  Assert(config.rpc_server_base_name == expected_rpc_server_name);
  Assert(config.rpc_server_suffix.empty());
  Assert(config.host_numbers == std::vector<int>());

  const char expected_shm_name[] = "/hermes_buffer_pool_";
  Assert(strncmp(config.buffer_pool_shmem_name, expected_shm_name,
                 sizeof(expected_shm_name)) == 0);

  Assert(config.default_placement_policy ==
         hermes::api::PlacementPolicy::kMinimizeIoTime);

  Assert(config.is_shared_device[0] == 0);
  Assert(config.is_shared_device[1] == 0);
  Assert(config.is_shared_device[2] == 0);
  Assert(config.is_shared_device[3] == 0);


  for (int i = 0; i < config.num_devices; ++i) {
    Assert(config.bo_capacity_thresholds_mb[i][0] == 0);
    int max_capacity_mb = (int)((f32)config.capacities[i] / 1024.0f / 1024.0f);
    Assert(config.bo_capacity_thresholds_mb[i][1] == max_capacity_mb);
  }

  Assert(config.bo_num_threads == 4);
}

}  // namespace testing
}  // namespace hermes

int main(int argc, char **argv) {
  if (argc < 2) {
    fprintf(stderr, "Expected a path to a hermes.conf file\n");
    exit(-1);
  }

  const size_t kConfigMemorySize = KILOBYTES(16);
  u8 config_memory[kConfigMemorySize];
  Arena arena = {};
  hermes::InitArena(&arena, kConfigMemorySize, config_memory);

  hermes::testing::TestDefaultConfig(&arena, argv[1]);
  hermes::testing::TestParseRangeList(&arena);
  hermes::testing::TestCapacityValues(&arena);
  hermes::testing::TestBlockSizes(&arena);

  return 0;
}
