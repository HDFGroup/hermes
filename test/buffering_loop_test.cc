#include <assert.h>

#include <mpi.h>

#include "common.h"
#include "hermes.h"
#include "bucket.h"
#include "buffer_pool.h"

namespace hapi = hermes::api;

int main(int argc, char **argv) {

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  hermes::SharedMemoryContext context = {};

  if (world_rank == 0) {
    hermes::Config config = {};
    InitTestConfig(&config);
    context = InitHermesCore(&config, true);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  if (world_rank != 0) {
    hermes::SharedMemoryContext context = hermes::InitHermesClient(world_rank,
                                                                   false);

    std::shared_ptr<hapi::Hermes> hstate = std::make_shared<hapi::Hermes>();
    hapi::Context ctx;
    hapi::Bucket bucket(std::string("test_bucket"), hstate);
    hapi::Blob put_data;
    bucket.Put("", put_data, ctx);

    hapi::Blob get_result;
    size_t blob_size = bucket.Get("", get_result, ctx);
    get_result.reserve(blob_size);
    blob_size = bucket.Get("", get_result, ctx);

    bucket.Release(ctx);
  }

  MPI_Finalize();

  return 0;
}
