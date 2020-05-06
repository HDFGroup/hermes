#include <memory>
#include <string>

#include <mpi.h>

#include "test_utils.h"
#include "hermes.h"
#include "common.h"
#include "bucket.h"

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
    int app_rank = hermes->GetProcessRank();
    // int app_size = hermes->GetNumProcesses();

    hapi::Context ctx;
    hapi::Bucket bucket(std::string("test_bucket_") + std::to_string(app_rank),
                        hermes, ctx);

    hapi::Blob put_data(KILOBYTES(4), std::to_string(app_rank)[0]);

    std::string blob_name = "test_blob" + std::to_string(app_rank);
    bucket.Put(blob_name, put_data, ctx);

    hapi::Blob get_result;
    size_t blob_size = bucket.Get(blob_name, get_result, ctx);
    get_result.resize(blob_size);
    blob_size = bucket.Get(blob_name, get_result, ctx);

    assert(put_data == get_result);

    bucket.Destroy(ctx);
  } else {
    // Hermes core
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
