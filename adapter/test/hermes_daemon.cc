#include <mpi.h>

#include <cstdlib>

#include "hermes.h"

/**
 * kHermesConf env variable is used to define path to kHermesConf in adapters.
 * This is used for initialization of Hermes.
 */
const char* kHermesConf = "HERMES_CONF";

int main(int argc, char* argv[]) {
  MPI_Init(&argc, &argv);
  char* hermes_config = nullptr;
  if (argc > 1) {
    hermes_config = argv[1];
  } else {
    hermes_config = getenv(kHermesConf);
  }
  auto hermes = hermes::InitHermesDaemon(hermes_config);
  hermes->RunDaemon();

  MPI_Finalize();

  return 0;
}
