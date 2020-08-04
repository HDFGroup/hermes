#include <string>
#include <vector>

#include "common.h"
#include "test_utils.h"

/**
 * @file buffer_pool_test.cc
 *
 * This is an example of starting a Hermes daemon that will service a FUSE
 * adapter. When run with MPI, it should be configured to run one process per
 * node.
 */

namespace hapi = hermes::api;

void TestGetBuffers(hapi::Hermes *hermes) {
  using namespace hermes;
  SharedMemoryContext *context = &hermes->context_;
  BufferPool *pool = GetBufferPoolFromContext(context);
  TierID tier_id = 0;
  i32 block_size = pool->block_sizes[tier_id];

  {
    // A request smaller than the block size should return 1 buffer
    TieredSchema schema{std::make_pair(block_size / 2, tier_id)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size() == 1);
    LocalReleaseBuffers(context, ret);
  }

  {
    // A request larger than the available space should return no buffers
    TieredSchema schema{std::make_pair(GIGABYTES(3), tier_id)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size() == 0);
  }

  {
    // Get all buffers in the tier
    UpdateGlobalSystemViewState(context, &hermes->rpc_);
    std::vector<u64> global_state = GetGlobalTierCapacities(context,
                                                            &hermes->rpc_);
    TieredSchema schema{std::make_pair(global_state[tier_id], tier_id)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size());
    // The next request should fail
    TieredSchema failed_schema{std::make_pair(1, tier_id)};
    std::vector<BufferID> failed_request = GetBuffers(context, failed_schema);
    Assert(failed_request.size() == 0);
    LocalReleaseBuffers(context, ret);
  }
}

void PrintUsage(char *program) {
  fprintf(stderr, "Usage %s -[b] [-f <path>]\n", program);
  fprintf(stderr, "  -b\n");
  fprintf(stderr, "     Run GetBuffers test.\n");
  fprintf(stderr, "  -f <path>\n");
  fprintf(stderr, "     Path to a Hermes configuration file.\n");
}

int main(int argc, char **argv) {

  int option = -1;
  char *config_file = 0;
  bool test_get_buffers = false;

  while ((option = getopt(argc, argv, "bf:")) != -1) {
    switch (option) {
      case 'b': {
        test_get_buffers = true;
        break;
      }
      case 'f': {
        config_file = optarg;
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

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermesDaemon(config_file);

  if (test_get_buffers) {
    InitFilesForBuffering(&hermes->context_, hermes->comm_.first_on_node);
    TestGetBuffers(hermes.get());
    hermes::RpcCall<void>(&hermes->rpc_, hermes->rpc_.node_id, "RemoteFinalize");
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
