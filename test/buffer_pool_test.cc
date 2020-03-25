#include <string>

#include "common.h"
#include "hermes_types.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "communication.h"

/**
 * @file buffer_pool_test.cc
 *
 * This is an example of what needs to happen in the Hermes core initialization
 * to set up and run a BufferPool.
 */

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-r] [-b arg] [-n arg]\n", program);
  fprintf(stderr, "  -d arg\n");
  fprintf(stderr, "     The path to a buffering directory.\n");
  fprintf(stderr, "  -n arg\n");
  fprintf(stderr, "     The number of RPC threads to start\n");
  fprintf(stderr, "  -r\n");
  fprintf(stderr, "     Start RPC server.\n");
}

int main(int argc, char **argv) {

  using namespace hermes;

  Config config = {};
  InitTestConfig(&config);

  int option = -1;
  bool start_rpc_server = false;
  i32 num_rpc_threads = 0;
  std::string buffering_path;

  while ((option = getopt(argc, argv, "d:n:r")) != -1) {
    switch (option) {
      case 'd': {
        buffering_path = std::string(optarg);
        for (int i = 0; i < config.num_tiers; ++i) {
          if (i == 0) {
            // NOTE(chogan): No mount point for RAM
            config.mount_points[i] = "";
          } else {
            // NOTE(chogan): For now just use the path passed in as the mount
            // point for all file Tiers.
            config.mount_points[i] = buffering_path.c_str();
          }
        }
        break;
      }
      case 'n': {
        num_rpc_threads = atoi(optarg);
        break;
      }
      case 'r': {
        start_rpc_server = true;
        break;
      }
      default:
        PrintUsage(argv[0]);
    }
  }

  if (optind < argc) {
    fprintf(stderr, "Got unknown argument '%s'.\n", argv[optind]);
    PrintUsage(argv[0]);
  }

  // TODO(chogan): Call InitCommunication before this
  SharedMemoryContext context = InitHermesCore(&config, NULL, NULL,
                                               start_rpc_server,
                                               num_rpc_threads);

  if (!start_rpc_server) {
    std::cin.get();
  }

  munmap(context.shm_base, context.shm_size);
  shm_unlink(config.buffer_pool_shmem_name);
  context.comm_api.finalize(&context.comm_state);

  return 0;
}
