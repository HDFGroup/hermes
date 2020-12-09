#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "buffer_pool_internal.h"
#include "metadata_management_internal.h"
#include "utils.h"
#include "test_utils.h"

/**
 * @file buffer_pool_test.cc
 *
 * This is an example of starting a Hermes daemon that will service a FUSE
 * adapter. When run with MPI, it should be configured to run one process per
 * node.
 */

using hermes::api::Hermes;

void TestGetBuffers(Hermes *hermes) {
  using namespace hermes;  // NOLINT(*)
  SharedMemoryContext *context = &hermes->context_;
  BufferPool *pool = GetBufferPoolFromContext(context);
  DeviceID device_id = 0;
  i32 block_size = pool->block_sizes[device_id];

  {
    // A request smaller than the block size should return 1 buffer
    PlacementSchema schema{std::make_pair(block_size / 2, device_id)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size() == 1);
    LocalReleaseBuffers(context, ret);
  }

  {
    // A request larger than the available space should return no buffers
    PlacementSchema schema{std::make_pair(GIGABYTES(3), device_id)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size() == 0);
  }

  {
    // Get all buffers on the device
    UpdateGlobalSystemViewState(context, &hermes->rpc_);
    std::vector<u64> global_state = GetGlobalDeviceCapacities(context,
                                                              &hermes->rpc_);
    PlacementSchema schema{std::make_pair(global_state[device_id], device_id)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size());
    // The next request should fail
    PlacementSchema failed_schema{std::make_pair(1, device_id)};
    std::vector<BufferID> failed_request = GetBuffers(context, failed_schema);
    Assert(failed_request.size() == 0);
    LocalReleaseBuffers(context, ret);
  }
}

void TestGetBandwidths(hermes::SharedMemoryContext *context) {
  using namespace hermes;  // NOLINT(*)
  std::vector<f32> bandwidths = GetBandwidths(context);
  Config config;
  InitDefaultConfig(&config);
  for (size_t i = 0; i < bandwidths.size(); ++i) {
    Assert(bandwidths[i] == config.bandwidths[i]);
    Assert(bandwidths.size() == (size_t)config.num_devices);
  }
}

void TestSwap(std::shared_ptr<Hermes> hermes) {
  namespace hapi = hermes::api;
  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRandom;
  hapi::Bucket bucket(std::string("swap_bucket"), hermes, ctx);
  hapi::Blob data(MEGABYTES(1), 'x');
  std::string blob_name("swap_blob");
  bucket.Put(blob_name, data, ctx);

  Assert(bucket.ContainsBlob(blob_name));

  hapi::Blob get_result;
  size_t blob_size = bucket.Get(blob_name, get_result, ctx);
  get_result.resize(blob_size);
  blob_size = bucket.Get(blob_name, get_result, ctx);

  Assert(get_result == data);

  bucket.Destroy(ctx);
}

void TestBufferOrganizer(std::shared_ptr<Hermes> hermes) {
  namespace hapi = hermes::api;
  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRandom;
  hapi::Bucket bucket(std::string("bo_bucket"), hermes, ctx);

  // NOTE(chogan): Fill our single buffer with a blob.
  hapi::Blob data1(KILOBYTES(4), 'x');
  std::string blob1_name("bo_blob1");
  bucket.Put(blob1_name, data1, ctx);
  Assert(bucket.ContainsBlob(blob1_name));

  // NOTE(chogan): Try to put another blob, which will go to the swap space
  // since the hierarchy is full.
  hapi::Blob data2(KILOBYTES(4), 'y');
  std::string blob2_name("bo_blob2");
  bucket.Put(blob2_name, data2, ctx);
  Assert(bucket.BlobIsInSwap(blob2_name));

  // NOTE(chogan): Delete the first blob, which will make room for the second,
  // and the buffer organizer should move it from swap space to the hierarchy.
  bucket.DeleteBlob(blob1_name, ctx);

  // NOTE(chogan): Give the BufferOrganizer time to finish.
  std::this_thread::sleep_for(std::chrono::seconds(2));

  Assert(bucket.ContainsBlob(blob2_name));
  Assert(!bucket.BlobIsInSwap(blob2_name));

  hapi::Blob get_result;
  size_t blob_size = bucket.Get(blob2_name, get_result, ctx);
  get_result.resize(blob_size);
  blob_size = bucket.Get(blob2_name, get_result, ctx);

  Assert(get_result == data2);

  bucket.Destroy(ctx);
}

void PrintUsage(char *program) {
  fprintf(stderr, "Usage %s -[b] [-f <path>]\n", program);
  fprintf(stderr, "  -b\n");
  fprintf(stderr, "     Run GetBuffers test.\n");
  fprintf(stderr, "  -f <path>\n");
  fprintf(stderr, "     Path to a Hermes configuration file.\n");
  fprintf(stderr, "  -s\n");
  fprintf(stderr, "     Test the functionality of the swap target.\n");
  fprintf(stderr, "  -x\n");
  fprintf(stderr, "     Start a Hermes server as a daemon.\n");
}

int main(int argc, char **argv) {
  int option = -1;
  char *config_file = 0;
  bool test_get_buffers = false;
  bool test_swap = false;
  bool start_server = false;

  while ((option = getopt(argc, argv, "bf:sx")) != -1) {
    switch (option) {
      case 'b': {
        test_get_buffers = true;
        break;
      }
      case 'f': {
        config_file = optarg;
        break;
      }
      case 's': {
        test_swap = true;
        break;
      }
      case 'x': {
        start_server = true;
        break;
      }
      default:
        PrintUsage(argv[0]);
        return 1;
    }
  }

  if (optind < argc) {
    fprintf(stderr, "non-option ARGV-elements: ");
    while (optind < argc) {
      fprintf(stderr, "%s ", argv[optind++]);
    }
    fprintf(stderr, "\n");
  }

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  if (test_get_buffers) {
    std::shared_ptr<Hermes> hermes = hermes::InitHermesDaemon(config_file);
    TestGetBuffers(hermes.get());
    TestGetBandwidths(&hermes->context_);
    hermes->Finalize(true);
  }

  if (test_swap) {
    hermes::Config config = {};
    InitDefaultConfig(&config);
    // NOTE(chogan): Make capacities small so that a Put of 1MB will go to swap
    // space. After metadata, this configuration gives us 1 4KB RAM buffer.
    config.capacities[0] = KILOBYTES(32);
    config.capacities[1] = 8;
    config.capacities[2] = 8;
    config.capacities[3] = 8;
    config.desired_slab_percentages[0][0] = 1;
    config.desired_slab_percentages[0][1] = 0;
    config.desired_slab_percentages[0][2] = 0;
    config.desired_slab_percentages[0][3] = 0;
    config.arena_percentages[hermes::kArenaType_BufferPool] = 0.5;
    config.arena_percentages[hermes::kArenaType_MetaData] = 0.5;

    std::shared_ptr<Hermes> hermes = hermes::InitHermesDaemon(&config);
    TestSwap(hermes);
    TestBufferOrganizer(hermes);
    hermes->Finalize(true);
  }

  if (start_server) {
    std::shared_ptr<Hermes> hermes = hermes::InitHermesDaemon(config_file);
    hermes->Finalize();
  }

  MPI_Finalize();

  return 0;
}
