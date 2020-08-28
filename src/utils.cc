#include "utils.h"

namespace hermes {

size_t RoundUpToMultiple(size_t val, size_t multiple) {
  if (multiple == 0) {
    return val;
  }

  size_t result = val;
  size_t remainder = val % multiple;

  if (remainder != 0) {
    result += multiple - remainder;
  }

  return result;
}

size_t RoundDownToMultiple(size_t val, size_t multiple) {
  if (multiple == 0) {
    return val;
  }

  size_t result = val;
  size_t remainder = val % multiple;
  result -= remainder;

  return result;
}

void InitDefaultConfig(Config *config) {
  config->num_devices = 4;
  config->num_targets = 4;
  assert(config->num_devices < kMaxDevices);

  for (int dev = 0; dev < config->num_devices; ++dev) {
    config->capacities[dev] = MEGABYTES(50);
    config->block_sizes[dev] = KILOBYTES(4);
    config->num_slabs[dev] = 4;

    config->slab_unit_sizes[dev][0] = 1;
    config->slab_unit_sizes[dev][1] = 4;
    config->slab_unit_sizes[dev][2] = 16;
    config->slab_unit_sizes[dev][3] = 32;

    config->desired_slab_percentages[dev][0] = 0.25f;
    config->desired_slab_percentages[dev][1] = 0.25f;
    config->desired_slab_percentages[dev][2] = 0.25f;
    config->desired_slab_percentages[dev][3] = 0.25f;
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

  config->mount_points[0] = "";
  config->mount_points[1] = "./";
  config->mount_points[2] = "./";
  config->mount_points[3] = "./";

  config->rpc_server_base_name = "localhost";
  config->rpc_server_suffix = "";
  config->rpc_protocol = "ofi+sockets";
  config->rpc_domain = "";
  config->rpc_port = 8080;
  config->rpc_num_threads = 1;

  config->max_buckets_per_node = 16;
  config->max_vbuckets_per_node = 8;
  config->system_view_state_update_interval_ms = 100;

  const char buffer_pool_shmem_name[] = "/hermes_buffer_pool_";
  size_t shmem_name_size = strlen(buffer_pool_shmem_name);
  for (size_t i = 0; i < shmem_name_size; ++i) {
    config->buffer_pool_shmem_name[i] = buffer_pool_shmem_name[i];
  }
  config->buffer_pool_shmem_name[shmem_name_size] = '\0';
}
}  // namespace hermes
