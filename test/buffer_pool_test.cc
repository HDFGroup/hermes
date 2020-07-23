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

int main(int argc, char **argv) {

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermesDaemon(config_file);
  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
