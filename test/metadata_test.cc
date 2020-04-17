#include <mpi.h>

#include "metadata_management.h"
#include "common.h"

namespace hapi = hermes::api;

int main(int argc, char **argv) {
  using namespace hermes;

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes();

  if (hermes->IsApplicationCore()) {
    // int app_rank = hermes->GetProcessRank();
    // int app_size = hermes->GetNumProcesses();

    // TODO(chogan): Build MetadataManager in InitBufferPool

  } else {
    // Hermes core
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
