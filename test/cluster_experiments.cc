#include <assert.h>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "test_utils.h"
#include "common.h"

namespace hapi = hermes::api;


int main(int argc, char **argv) {

  if (argc < 2) {
    fprintf(stderr, "Expected a path to a configuration file\n");
    exit(-1);
  }

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(argv[1]);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    // int app_size = hermes->GetNumProcesses();

    // One rank Puts a Blob and every other rank in the cluster tries to Get it
    hapi::Context ctx;
    hapi::Bucket bucket(std::string("test_bucket"), hermes, ctx);

    const size_t put_size = MEGABYTES(4);
    hapi::Blob blob(put_size);
    for (size_t i = 0; i < put_size; ++i) {
      blob[i] = (unsigned char)i;
    }

    std::string blob_name = "test_blob";

    if (app_rank == 0) {
      bucket.Put(blob_name, blob, ctx);
    }

    hermes->AppBarrier();

    int iterations = 5000;
    for (int iter = 0; iter < iterations; ++iter) {
      hapi::Blob get_result;
      size_t blob_size = bucket.Get(blob_name, get_result, ctx);
      get_result.resize(blob_size);
      blob_size = bucket.Get(blob_name, get_result, ctx);
      Assert(blob == get_result);
    }

    if (app_rank != 0) {
      bucket.Close(ctx);
    }
    hermes->AppBarrier();

    if (app_rank == 0) {
      bucket.Destroy(ctx);
    }
    hermes->AppBarrier();

  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
