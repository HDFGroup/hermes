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

  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes();

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    size_t data_size = KILOBYTES(4);
    size_t bytes_per_rank = data_size / app_size ;
    size_t remaining_bytes = data_size % app_size;

    if (app_rank == app_size - 1) {
      bytes_per_rank += remaining_bytes;
    }

    hapi::Context ctx;
    hapi::Bucket bucket(std::string("test_bucket"), hermes, ctx);

    hapi::Blob put_data(bytes_per_rank, rand() % 255);

    std::string blob_name = "test_blob" + std::to_string(app_rank);
    bucket.Put(blob_name, put_data, ctx);

    hapi::Blob get_result;
    size_t blob_size = bucket.Get(blob_name, get_result, ctx);
    get_result.resize(blob_size);
    blob_size = bucket.Get(blob_name, get_result, ctx);

    assert(put_data == get_result);

    bucket.Release(ctx);

  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
