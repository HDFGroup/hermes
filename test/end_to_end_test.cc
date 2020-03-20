#include <assert.h>

#include <mpi.h>

#include "common.h"
#include "hermes.h"
#include "bucket.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"

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

  MPI_Comm app_comm = 0;
  hermes::SharedMemoryContext context = {};

  if (world_rank == 0) {
    hermes::Config config = {};
    InitTestConfig(&config);
    config.mount_points[0] = "";
    config.mount_points[1] = "./";
    config.mount_points[2] = "./";
    config.mount_points[3] = "./";
    context = InitHermesCore(&config, false);

  } else {
    MPI_Comm_split(MPI_COMM_WORLD, (int)hermes::ProcessKind::kApp, world_rank,
                   &app_comm);
    MPI_Barrier(MPI_COMM_WORLD);

    int app_rank;
    MPI_Comm_rank(app_comm, &app_rank);
    int app_size;
    MPI_Comm_size(app_comm, &app_size);

    context = hermes::InitHermesClient(world_rank, true);

    size_t data_size = 4 * 1024;
    size_t bytes_per_rank = data_size / app_size ;
    size_t remaining_bytes = data_size % app_size;

    if (app_rank == app_size - 1) {
      bytes_per_rank += remaining_bytes;
    }

    std::shared_ptr<hapi::Hermes> hstate =
      std::make_shared<hapi::Hermes>(context);
    hapi::Context ctx;
    hapi::Bucket bucket(std::string("test_bucket"), hstate);

    hapi::Blob put_data;
    put_data.resize(bytes_per_rank);

    for (size_t i = 0; i < bytes_per_rank; ++i) {
      put_data[i] = i % 0xFF;
    }

    std::string blob_name = ("test_blob" + std::to_string(app_rank));
    bucket.Put(blob_name, put_data, ctx);

    hapi::Blob get_result;
    size_t blob_size = bucket.Get(blob_name, get_result, ctx);
    get_result.resize(blob_size);
    blob_size = bucket.Get(blob_name, get_result, ctx);

    assert(put_data == get_result);

    bucket.Release(ctx);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();

  return 0;
}
