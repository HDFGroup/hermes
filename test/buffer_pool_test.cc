#include <string>

#include "common.h"

/**
 * @file buffer_pool_test.cc
 *
 * This is an example of starting a Hermes daemon that will service a FUSE
 * adapter. When run with MPI, it should be configured to run one process per
 * node.
 */

namespace hapi = hermes::api;

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-d arg] [-n arg]\n", program);
  fprintf(stderr, "  -d arg\n");
  fprintf(stderr, "     The path to a buffering directory.\n");
  fprintf(stderr, "     Defaults to './'\n");
  fprintf(stderr, "  -n arg\n");
  fprintf(stderr, "     The number of RPC threads to start\n");
  fprintf(stderr, "  -r arg\n");
  fprintf(stderr, "     The Mercury-compatible RPC server name.\n");
  fprintf(stderr, "     Defaults to 'na+sm'.\n");
}

int main(int argc, char **argv) {

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  int option = -1;
  std::string buffering_path;
  std::string rpc_server_name = "sockets://localhost:8080";

  while ((option = getopt(argc, argv, "d:n:r:")) != -1) {
    switch (option) {
      case 'd': {
        buffering_path = std::string(optarg);
        break;
      }
      case 'r': {
        rpc_server_name = std::string(optarg);
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

  std::shared_ptr<hapi::Hermes> hermes =
    hermes::InitDaemon(buffering_path, rpc_server_name);

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
